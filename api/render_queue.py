"""Vercel serverless function — drains the Supabase coi_pdf_queue.

Runs on every cron tick. Picks up to N pending queue rows, renders the real
ACORD 25 (2016/03) for each cert via pypdf against the fillable template,
uploads the PDF to Supabase Storage (bucket coi-pdfs), marks the queue row
finished, and pings coi-bundle/certificates/:id/notify-requester so the
originating requester gets the PDF by email.

Hosted at https://<vercel-app>.vercel.app/api/render-queue
Vercel Cron hits it on the schedule in vercel.json.

Auth: expects `Authorization: Bearer $CRON_SECRET` — Vercel Cron sends this
automatically when CRON_SECRET is set as a project env var.

Environment (set in Vercel Dashboard → Project Settings → Env Vars):
  SUPABASE_URL                  https://dbtfifgcvgzrxmxnjtta.supabase.co
  SUPABASE_SERVICE_ROLE_KEY     <service role key>
  SUPABASE_DB_URL               postgresql://postgres:PASSWORD@db.<ref>.supabase.co:5432/postgres
  CRON_SECRET                   any long random string
  BATCH_SIZE                    (optional) default 10 — how many certs per tick
"""
from http.server import BaseHTTPRequestHandler
from datetime import date, datetime
import io
import json
import os
import sys
import traceback
from typing import Optional

import psycopg
import requests
from pypdf import PdfReader, PdfWriter
from pypdf.generic import BooleanObject, NameObject

# ── Config ─────────────────────────────────────────────────────────────
SUPABASE_URL = os.environ["SUPABASE_URL"]
SERVICE_ROLE = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
DB_URL       = os.environ["SUPABASE_DB_URL"]
CRON_SECRET  = os.environ.get("CRON_SECRET")

TEMPLATE_PATH = os.path.join(os.path.dirname(__file__), "templates", "acord25-2016-03.pdf")
BUCKET   = "coi-pdfs"
ON_STATE = "/1"
P        = "F[0].P1[0]."

PRODUCER_ENTITY = {
    "name": "Golden Sports by Corgi",
    "street": "2200 Guadalupe St, Ste 400",
    "city": "Austin", "state": "TX", "zip": "78705",
    "fax": "",
}
DEFAULT_AM = {"name": "Emily Yuan", "phone": "832-489-3247", "email": "ey@corgi.insure"}
AUTHORIZED_SIGNATURE_NAME = "Emily Yuan"
INSURER_A = {"name": "Sports & Entertainment Insurance Co.", "naic": "209410"}


# ── Helpers ────────────────────────────────────────────────────────────
def mdy(d):
    if not d:
        return ""
    if isinstance(d, str):
        try:
            d = datetime.fromisoformat(d.replace("Z", "+00:00"))
        except ValueError:
            return d
    return f"{d.month:02d}/{d.day:02d}/{d.year}"


def money(n):
    if n is None:
        return ""
    try:
        return f"${int(float(n)):,}"
    except (TypeError, ValueError):
        return str(n)


def am_for_org(org):
    am = org.get("account_manager") or {}
    return {
        "name":  am.get("name")  or DEFAULT_AM["name"],
        "phone": am.get("phone") or DEFAULT_AM["phone"],
        "email": am.get("email") or DEFAULT_AM["email"],
    }


def build_fill_map(cert, policy, org):
    """Same field mapping as scripts/coi/fill_acord25.py — kept in sync."""
    am = am_for_org(org)
    ngb_addr = org.get("address") or {}
    holder_addr = cert.get("holder_address") or {}
    ngb_name = cert.get("named_insured") or org.get("legal_name") or org.get("name") or ""
    cert_number = (
        (org.get("acronym") or (org.get("name") or "NGB").replace(" ", ""))[:8].upper()
        + f"-MC-{date.today().year}-"
        + str(cert["id"])[:4].upper()
    )
    is_claims_made = bool((policy.get("sublimits") or {}).get("claims_made"))
    ai_ends   = cert.get("ai_endorsements") or []
    is_ai     = bool(cert.get("additional_insured"))
    ai_name   = cert.get("ai_name") or ""
    ai_addr   = cert.get("ai_address") or {}
    has_waiver = "waiver_of_subrogation" in ai_ends
    has_pnc    = "primary_non_contributory" in ai_ends

    ops_lines = [
        f"MASTER CERTIFICATE - coverage afforded under {ngb_name} Master Policy No. "
        f"{policy.get('policy_number', '')}, effective {mdy(policy.get('effective_date'))} "
        f"through {mdy(policy.get('expiration_date'))}."
    ]
    if is_ai and ai_name:
        addr_line = ", ".join([p for p in [
            ai_addr.get("street"), ai_addr.get("city"),
            ai_addr.get("state"), ai_addr.get("zip"),
        ] if p])
        ops_lines.append(
            f"{ai_name} is included as an Additional Insured under the Commercial "
            f"General Liability Coverage Part per Form SEIC-19.01-111 where required "
            f"by written agreement with the Named Insured."
            + (f"\nAddress: {addr_line}." if addr_line else "")
        )
    if has_waiver:
        ops_lines.append(
            "Waiver of Subrogation applies in favor of the Certificate Holder "
            "per policy endorsement."
        )
    if has_pnc:
        ops_lines.append(
            "Coverage is primary and non-contributory with respect to the "
            "Certificate Holder where required by written agreement."
        )
    ops_lines.append(
        "Certificate holder is a member club of the Named Insured and is included "
        "on this Certificate as a matter of information only."
    )
    ops_lines.append(
        "This policy is issued by a Utah-domiciled captive insurer. Coverage hereunder "
        "is not subject to protection by the Utah Property and Casualty Insurance "
        "Guaranty Association or any similar state guaranty fund."
    )
    ops_text = "\n\n".join(ops_lines)

    return {
        P + "CertificateOfInsurance_CertificateNumberIdentifier_A[0]": cert_number,
        P + "CertificateOfInsurance_RevisionNumberIdentifier_A[0]":    "0",
        P + "Form_CompletionDate_A[0]":                                mdy(date.today()),
        P + "Producer_FullName_A[0]":                                  PRODUCER_ENTITY["name"],
        P + "Producer_MailingAddress_LineOne_A[0]":                    PRODUCER_ENTITY["street"],
        P + "Producer_MailingAddress_CityName_A[0]":                   PRODUCER_ENTITY["city"],
        P + "Producer_MailingAddress_StateOrProvinceCode_A[0]":        PRODUCER_ENTITY["state"],
        P + "Producer_MailingAddress_PostalCode_A[0]":                 PRODUCER_ENTITY["zip"],
        P + "Producer_FaxNumber_A[0]":                                 PRODUCER_ENTITY["fax"],
        P + "Producer_ContactPerson_FullName_A[0]":                    am["name"],
        P + "Producer_ContactPerson_PhoneNumber_A[0]":                 am["phone"],
        P + "Producer_ContactPerson_EmailAddress_A[0]":                am["email"],
        P + "NamedInsured_FullName_A[0]":                              ngb_name,
        P + "NamedInsured_MailingAddress_LineOne_A[0]":                ngb_addr.get("street", ""),
        P + "NamedInsured_MailingAddress_CityName_A[0]":               ngb_addr.get("city", ""),
        P + "NamedInsured_MailingAddress_StateOrProvinceCode_A[0]":    ngb_addr.get("state", ""),
        P + "NamedInsured_MailingAddress_PostalCode_A[0]":             ngb_addr.get("zip", ""),
        P + "Insurer_FullName_A[0]":                                   INSURER_A["name"],
        P + "Insurer_NAICCode_A[0]":                                   INSURER_A["naic"],
        P + "GeneralLiability_InsurerLetterCode_A[0]":                 "A",
        P + "GeneralLiability_CoverageIndicator_A[0]":                 ON_STATE,
        P + ("GeneralLiability_ClaimsMadeIndicator_A[0]" if is_claims_made
             else "GeneralLiability_OccurrenceIndicator_A[0]"):        ON_STATE,
        P + "GeneralLiability_GeneralAggregate_LimitAppliesPerPolicyIndicator_A[0]": ON_STATE,
        P + "Policy_GeneralLiability_PolicyNumberIdentifier_A[0]":     policy.get("policy_number", "") or "",
        P + "Policy_GeneralLiability_EffectiveDate_A[0]":              mdy(policy.get("effective_date") or cert.get("effective_date")),
        P + "Policy_GeneralLiability_ExpirationDate_A[0]":             mdy(policy.get("expiration_date") or cert.get("expiration_date")),
        P + "GeneralLiability_EachOccurrence_LimitAmount_A[0]":                     money(policy.get("per_occ_limit")),
        P + "GeneralLiability_FireDamageRentedPremises_EachOccurrenceLimitAmount_A[0]": money(2_000_000),
        P + "GeneralLiability_MedicalExpense_EachPersonLimitAmount_A[0]":           money(5_000),
        P + "GeneralLiability_PersonalAndAdvertisingInjury_LimitAmount_A[0]":       money(policy.get("per_occ_limit")),
        P + "GeneralLiability_GeneralAggregate_LimitAmount_A[0]":                   money(policy.get("aggregate_limit")),
        P + "GeneralLiability_ProductsAndCompletedOperations_AggregateLimitAmount_A[0]": money(policy.get("aggregate_limit")),
        P + "CertificateOfInsurance_GeneralLiability_AdditionalInsuredCode_A[0]":   "Y" if is_ai else "N",
        P + "Policy_GeneralLiability_SubrogationWaivedCode_A[0]":                   "Y" if has_waiver else "N",
        P + "CertificateOfLiabilityInsurance_ACORDForm_RemarkText_A[0]":            ops_text,
        P + "CertificateHolder_FullName_A[0]":                         cert.get("certificate_holder", "") or "",
        P + "CertificateHolder_MailingAddress_LineOne_A[0]":           holder_addr.get("street", ""),
        P + "CertificateHolder_MailingAddress_CityName_A[0]":          holder_addr.get("city", ""),
        P + "CertificateHolder_MailingAddress_StateOrProvinceCode_A[0]": holder_addr.get("state", ""),
        P + "CertificateHolder_MailingAddress_PostalCode_A[0]":        holder_addr.get("zip", ""),
        P + "Producer_AuthorizedRepresentative_Signature_A[0]":        AUTHORIZED_SIGNATURE_NAME,
    }


def fill_pdf(fill_map: dict) -> bytes:
    reader = PdfReader(TEMPLATE_PATH)
    if reader.is_encrypted:
        reader.decrypt("")
    writer = PdfWriter(clone_from=reader)
    if "/AcroForm" in writer._root_object:  # noqa: SLF001
        writer._root_object["/AcroForm"][NameObject("/NeedAppearances")] = BooleanObject(True)
    for page in writer.pages:
        writer.update_page_form_field_values(page, fill_map)
        annots = page.get("/Annots")
        if not annots:
            continue
        for annot_ref in annots:
            annot = annot_ref.get_object()
            if annot.get("/Subtype") != "/Widget" or annot.get("/FT") != "/Btn":
                continue
            parents = []
            t = annot.get("/T")
            if t:
                parents.append(str(t))
            p = annot.get("/Parent")
            while p is not None:
                po = p.get_object()
                pt = po.get("/T")
                if pt:
                    parents.append(str(pt))
                p = po.get("/Parent")
            full_name = ".".join(reversed(parents))
            desired = fill_map.get(full_name)
            if desired:
                annot[NameObject("/AS")] = NameObject(desired)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def upload(org_id: str, cert_id: str, pdf_bytes: bytes) -> Optional[str]:
    path = f"{org_id}/{cert_id}.pdf"
    r = requests.post(
        f"{SUPABASE_URL}/storage/v1/object/{BUCKET}/{path}",
        headers={
            "Authorization": f"Bearer {SERVICE_ROLE}",
            "apikey": SERVICE_ROLE,
            "Content-Type": "application/pdf",
            "x-upsert": "true",
        },
        data=pdf_bytes,
        timeout=30,
    )
    if not r.ok:
        print(f"upload failed {r.status_code}: {r.text}", file=sys.stderr)
        return None
    return f"{SUPABASE_URL}/storage/v1/object/public/{BUCKET}/{path}"


def notify_requester(cert_id: str):
    try:
        r = requests.post(
            f"{SUPABASE_URL}/functions/v1/coi-bundle/certificates/{cert_id}/notify-requester",
            headers={"Authorization": f"Bearer {SERVICE_ROLE}", "apikey": SERVICE_ROLE},
            timeout=30,
        )
        return r.ok
    except Exception as e:  # noqa: BLE001
        print(f"notify raised: {e}", file=sys.stderr)
        return False


def load_cert(conn, cert_id):
    row = conn.execute("""
      SELECT c.id, c.certificate_holder, c.named_insured, c.holder_address,
             c.additional_insured, c.ai_name, c.ai_address, c.ai_endorsements,
             c.effective_date, c.expiration_date, c.org_id,
             row_to_json(p) AS policy_json,
             row_to_json(o) AS org_json,
             row_to_json(club) AS club_json
        FROM certificates c
        JOIN policies      p    ON p.id = c.policy_id
        JOIN organizations o    ON o.id = c.org_id
        LEFT JOIN organizations club ON club.id = c.club_org_id
       WHERE c.id = %s
    """, (cert_id,)).fetchone()
    if not row:
        return None
    (cid, holder, ni, holder_addr, addl, ai_name, ai_addr, ai_ends,
     eff, exp, org_id, policy, org, club) = row
    if (not holder_addr or holder_addr == {}) and club and (club.get("address") or {}):
        holder_addr = club["address"]
        conn.execute(
            "UPDATE certificates SET holder_address = %s WHERE id = %s",
            (psycopg.types.json.Jsonb(holder_addr), cid),
        )
    cert = {
        "id": cid, "certificate_holder": holder, "named_insured": ni,
        "holder_address": holder_addr,
        "additional_insured": addl,
        "ai_name": ai_name, "ai_address": ai_addr or {},
        "ai_endorsements": list(ai_ends or []),
        "effective_date": eff, "expiration_date": exp,
        "org_id": org_id,
    }
    return cert, policy, org


def render_one(conn, cert_id: str) -> dict:
    loaded = load_cert(conn, cert_id)
    if not loaded:
        return {"cert_id": cert_id, "ok": False, "error": "cert not found"}
    cert, policy, org = loaded
    try:
        fill = build_fill_map(cert, policy, org)
        pdf_bytes = fill_pdf(fill)
        url = upload(str(cert["org_id"]), str(cert["id"]), pdf_bytes)
        if not url:
            return {"cert_id": cert_id, "ok": False, "error": "upload failed"}
        conn.execute(
            "UPDATE certificates SET document_url = %s, status = 'active' WHERE id = %s",
            (url, cert["id"]),
        )
        notify_requester(str(cert["id"]))
        return {"cert_id": cert_id, "ok": True, "url": url}
    except Exception as e:  # noqa: BLE001
        return {"cert_id": cert_id, "ok": False, "error": str(e)}


def drain_queue(batch_size: int) -> dict:
    results = []
    with psycopg.connect(DB_URL, autocommit=True) as conn:
        rows = conn.execute("""
          SELECT certificate_id FROM coi_pdf_queue
           WHERE finished_at IS NULL
           ORDER BY enqueued_at
           LIMIT %s
        """, (batch_size,)).fetchall()
        for (cid,) in rows:
            conn.execute(
                "UPDATE coi_pdf_queue SET started_at = NOW(), attempts = attempts + 1 "
                "WHERE certificate_id = %s",
                (cid,),
            )
            result = render_one(conn, str(cid))
            if result["ok"]:
                conn.execute(
                    "UPDATE coi_pdf_queue SET finished_at = NOW(), last_error = NULL "
                    "WHERE certificate_id = %s",
                    (cid,),
                )
            else:
                conn.execute(
                    "UPDATE coi_pdf_queue SET last_error = %s WHERE certificate_id = %s",
                    (result.get("error", "unknown"), cid),
                )
            results.append(result)
    return {"picked": len(results), "results": results}


# ── Vercel handler ─────────────────────────────────────────────────────
class handler(BaseHTTPRequestHandler):
    def _write(self, status: int, body: dict):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def _authorized(self) -> bool:
        if not CRON_SECRET:
            return True  # dev mode
        auth = self.headers.get("authorization", "")
        return auth == f"Bearer {CRON_SECRET}"

    def _handle(self):
        if not self._authorized():
            return self._write(401, {"error": "unauthorized"})
        batch_size = int(os.environ.get("BATCH_SIZE", "10"))
        try:
            result = drain_queue(batch_size)
            self._write(200, result)
        except Exception:  # noqa: BLE001
            self._write(500, {"error": traceback.format_exc()})

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()
