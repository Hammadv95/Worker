# COI PDF Worker (Vercel Cron)

Serverless Python function that drains `coi_pdf_queue` in the Supabase DB,
renders the real ACORD 25 (2016/03) for each cert against the fillable
template using `pypdf`, uploads the PDF to Supabase Storage, and pings
`coi-bundle/certificates/:id/notify-requester` so the originating requester
gets the PDF by email.

Runs on Vercel Cron every minute.

## One-time setup

### 1. Create the Vercel project

- **Vercel Dashboard** → https://vercel.com/beagle1 → **Add New… → Project**
- **Import Git Repository** → point at `Goldenprotection/project-genesis`
- **Root Directory:** `worker`
- **Framework Preset:** *Other*
- Do **not** deploy yet — set env vars first (below).

### 2. Env vars

Vercel Dashboard → Project → **Settings → Environment Variables**. Add each
of these for **Production**:

| Name                          | Value                                                                   |
|-------------------------------|-------------------------------------------------------------------------|
| `SUPABASE_URL`                | `https://dbtfifgcvgzrxmxnjtta.supabase.co`                              |
| `SUPABASE_SERVICE_ROLE_KEY`   | Supabase Dashboard → Settings → API → `service_role` key (JWT format)   |
| `SUPABASE_DB_URL`             | `postgresql://postgres:PASSWORD@db.dbtfifgcvgzrxmxnjtta.supabase.co:5432/postgres` |
| `CRON_SECRET`                 | Any random 64-char string. Vercel Cron auto-sends this as `Bearer` on every trigger. |
| `BATCH_SIZE`                  | *(optional)* `10` — how many certs per tick. Bigger = fewer ticks, longer per tick. |

### 3. Deploy

- Click **Deploy** in the Vercel Dashboard, or
- From your local machine:
  ```powershell
  cd C:\Users\hamma\project-genesis\worker
  vercel --prod
  ```

### 4. Verify

Check Vercel Dashboard → **Logs** after the first cron tick (within 1 min).
Expected line:
```
{"picked": 0, "results": []}
```
(Empty queue = nothing to render, which is correct on a fresh setup.)

## Testing the render path

To force a render, enqueue any pending cert manually via Supabase SQL Editor:

```sql
-- pick any is_template cert that has no PDF yet
INSERT INTO coi_pdf_queue (certificate_id)
SELECT id FROM certificates WHERE is_template AND document_url IS NULL LIMIT 1;
```

Wait ≤ 1 min. Then check:

```sql
SELECT id, document_url, status FROM certificates WHERE id = '<the-cert-id>';
```

`document_url` should be populated and `status = 'active'`.

## What happens on every cron tick

1. Vercel wakes the function (~500ms cold-start)
2. Function connects to Supabase DB
3. `SELECT certificate_id FROM coi_pdf_queue WHERE finished_at IS NULL LIMIT $BATCH_SIZE`
4. For each row:
   - Load cert + policy + org from DB
   - Fall back to org.address if cert.holder_address is empty (self-heal)
   - Build the ACORD 25 fill map (checkbox + text values)
   - Fill the template via `pypdf.PdfWriter`
   - Upload to Supabase Storage bucket `coi-pdfs`
   - `UPDATE certificates SET document_url = ..., status = 'active'`
   - `UPDATE coi_pdf_queue SET finished_at = NOW()`
   - `POST /coi-bundle/certificates/:id/notify-requester` → Resend email

Cron minimum interval on Pro plan is 1 minute — reasonable for user-facing
approval flows. If someone approves an endorsement, they see the emailed PDF
within ~60 seconds.

## Cost

- **Vercel Cron**: included in Pro plan.
- **Serverless invocations**: 30 ticks/hr × 720 hr/mo = 21,600 invocations/mo.
  Well under the 100k/mo Pro allowance.
- **Compute time**: ~2-5 seconds per tick empty, up to ~30s if `BATCH_SIZE=10`
  and all render successfully. Comfortably under the 300s function timeout.

## Rolling back

Delete the Vercel project or disable the cron in `vercel.json`. Cert
approvals still succeed (they just enqueue), but PDFs stay in `pending`
until you either restart the worker or run
`python scripts/coi/fill_acord25.py --queue` locally.
