# ============================================================
# FILE: utils/stripe_webhook.py
# PURPOSE: Stripe webhook handler documentation and code.
#
# WHY WEBHOOKS?
#   Webhooks let Stripe tell your server when important events
#   happen (new subscription, cancellation, payment failure)
#   even when the user isn't actively using the app.
#
# THE PROBLEM WITH STREAMLIT:
#   Streamlit is a pure frontend framework — it can't receive
#   HTTP POST requests from Stripe.  You need a separate tiny
#   server to handle webhooks.
#
# MVP APPROACH (no webhooks needed):
#   For an MVP, the app verifies subscription status directly
#   via the Stripe API on each page load (with 30-min caching
#   in session state).  This works great for small user counts.
#
# PRODUCTION APPROACH:
#   Set up a minimal Flask or FastAPI webhook endpoint alongside
#   your Streamlit app.  Stripe sends POST requests to it, and
#   it updates the SQLite database directly.
#
# LOCAL TESTING:
#   Use the Stripe CLI to forward webhooks to localhost:
#     stripe listen --forward-to localhost:5000/stripe/webhook
#
# ============================================================

# ============================================================
# SECTION: Option A — Flask Webhook (minimal, ~50 lines)
# ============================================================
#
# Install Flask: pip install flask
#
# Save this as webhook_server.py and run with:
#   python webhook_server.py
#
# Then configure your Stripe Dashboard webhook to point to:
#   https://your-domain.com/stripe/webhook
#
# ── webhook_server.py ────────────────────────────────────────
#
#   import os
#   import sqlite3
#   import datetime
#   from flask import Flask, request, jsonify
#   import stripe
#
#   app = Flask(__name__)
#   stripe.api_key = os.environ["STRIPE_SECRET_KEY"]
#   WEBHOOK_SECRET = os.environ["STRIPE_WEBHOOK_SECRET"]
#   DB_PATH = "db/smartai_nba.db"
#
#   @app.route("/stripe/webhook", methods=["POST"])
#   def stripe_webhook():
#       payload = request.get_data()
#       sig_header = request.headers.get("Stripe-Signature", "")
#
#       try:
#           event = stripe.Webhook.construct_event(
#               payload, sig_header, WEBHOOK_SECRET
#           )
#       except (ValueError, stripe.error.SignatureVerificationError):
#           return jsonify({"error": "Invalid payload"}), 400
#
#       _handle_stripe_event(event)
#       return jsonify({"status": "ok"}), 200
#
#   def _handle_stripe_event(event):
#       """Process a Stripe webhook event and update the database."""
#       event_type = event["type"]
#       data = event["data"]["object"]
#
#       if event_type == "checkout.session.completed":
#           # New subscription created after checkout
#           sub_id = data.get("subscription")
#           cust_id = data.get("customer")
#           email = data.get("customer_email") or data.get("customer_details", {}).get("email", "")
#           if sub_id:
#               sub = stripe.Subscription.retrieve(sub_id)
#               _upsert_subscription(
#                   subscription_id=sub_id,
#                   customer_id=cust_id,
#                   customer_email=email,
#                   status=sub.status,
#                   period_start=datetime.datetime.fromtimestamp(sub.current_period_start).isoformat(),
#                   period_end=datetime.datetime.fromtimestamp(sub.current_period_end).isoformat(),
#               )
#
#       elif event_type in ("customer.subscription.updated",
#                           "customer.subscription.deleted"):
#           # Subscription status changed (upgrade, downgrade, cancel)
#           sub_id = data.get("id")
#           status = data.get("status", "cancelled")
#           period_end = ""
#           if data.get("current_period_end"):
#               period_end = datetime.datetime.fromtimestamp(
#                   data["current_period_end"]
#               ).isoformat()
#           if sub_id:
#               with sqlite3.connect(DB_PATH) as conn:
#                   conn.execute(
#                       """
#                       UPDATE subscriptions
#                          SET status = ?, current_period_end = ?, updated_at = datetime('now')
#                        WHERE subscription_id = ?
#                       """,
#                       (status, period_end, sub_id),
#                   )
#                   conn.commit()
#
#   def _upsert_subscription(subscription_id, customer_id, customer_email,
#                             status, period_start, period_end):
#       """Insert or update a subscription record in the database."""
#       with sqlite3.connect(DB_PATH) as conn:
#           conn.execute(
#               """
#               INSERT INTO subscriptions
#                   (subscription_id, customer_id, customer_email,
#                    status, current_period_start, current_period_end, updated_at)
#               VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
#               ON CONFLICT(subscription_id) DO UPDATE SET
#                   status               = excluded.status,
#                   customer_email       = excluded.customer_email,
#                   current_period_start = excluded.current_period_start,
#                   current_period_end   = excluded.current_period_end,
#                   updated_at           = datetime('now')
#               """,
#               (subscription_id, customer_id, customer_email,
#                status, period_start, period_end),
#           )
#           conn.commit()
#
#   if __name__ == "__main__":
#       app.run(port=5000)
#
# ── end webhook_server.py ────────────────────────────────────


# ============================================================
# SECTION: Option B — FastAPI Webhook (async, production-ready)
# ============================================================
#
# Install FastAPI: pip install fastapi uvicorn
#
# Run with: uvicorn webhook_fastapi:app --port 5000
#
# ── webhook_fastapi.py ───────────────────────────────────────
#
#   from fastapi import FastAPI, Request, HTTPException
#   import stripe, os
#
#   app = FastAPI()
#   stripe.api_key = os.environ["STRIPE_SECRET_KEY"]
#   WEBHOOK_SECRET = os.environ["STRIPE_WEBHOOK_SECRET"]
#
#   @app.post("/stripe/webhook")
#   async def webhook(request: Request):
#       payload = await request.body()
#       sig = request.headers.get("stripe-signature", "")
#       try:
#           event = stripe.Webhook.construct_event(payload, sig, WEBHOOK_SECRET)
#       except Exception:
#           raise HTTPException(status_code=400, detail="Invalid signature")
#       # ... same _handle_stripe_event() logic as Flask version above
#       return {"status": "ok"}
#
# ── end webhook_fastapi.py ───────────────────────────────────


# ============================================================
# SECTION: Stripe CLI Local Testing
# ============================================================
#
# 1. Install Stripe CLI: https://stripe.com/docs/stripe-cli
# 2. Login: stripe login
# 3. Forward webhooks to your local webhook server:
#      stripe listen --forward-to localhost:5000/stripe/webhook
# 4. The CLI will print a webhook signing secret — set it:
#      export STRIPE_WEBHOOK_SECRET=whsec_...
# 5. Trigger test events:
#      stripe trigger checkout.session.completed
#      stripe trigger customer.subscription.updated
#      stripe trigger customer.subscription.deleted
#
# ============================================================


# ============================================================
# SECTION: Deployment Notes
# ============================================================
#
# STREAMLIT CLOUD:
#   Streamlit Cloud does not support background servers.
#   Use the MVP approach (no webhooks) — the app verifies
#   subscription status directly via Stripe API on page load.
#   For production, host the Flask webhook server on:
#     • Railway (railway.app) — free tier available
#     • Render (render.com)   — free tier available
#     • Fly.io (fly.io)       — free tier available
#
# ENVIRONMENT VARIABLES TO SET:
#   STRIPE_SECRET_KEY      = sk_live_...   (or sk_test_... for testing)
#   STRIPE_PUBLISHABLE_KEY = pk_live_...   (or pk_test_... for testing)
#   STRIPE_PRICE_ID        = price_...     (from Stripe Dashboard → Products)
#   STRIPE_WEBHOOK_SECRET  = whsec_...     (from Stripe Dashboard → Webhooks)
#   APP_URL                = https://your-app.streamlit.app
#
# ============================================================
