# E-commerce Event Schemas

Events produced by **OrderService** and **PaymentService** and consumed by **NotificationService**. All fields are JSON.

---

## Order events (topic: `order-events`)

Produced by **OrderService**. Key: `orderId`.

### ORDER_CREATED

```json
{
  "event_id": "a8a1c867-05c3-4d43-9884-f7b55f1f0a7c",
  "event_type": "ORDER_CREATED",
  "timestamp": 1724684407,
  "order_id": "ORD-1001",
  "customer_id": "CUST-42",
  "items": [
    { "sku": "SKU-001", "qty": 2, "unit_price": 29.99 },
    { "sku": "SKU-002", "qty": 1, "unit_price": 99.50 }
  ],
  "total_amount": 159.48,
  "currency": "USD",
  "status": "CREATED"
}
```

### ORDER_CANCELLED

```json
{
  "event_id": "b9b2d978-16d4-5e54-a995-g8c66g2g1b8d",
  "event_type": "ORDER_CANCELLED",
  "timestamp": 1724684500,
  "order_id": "ORD-1001",
  "reason": "customer_request",
  "status": "CANCELLED"
}
```

---

## Payment events (topic: `payment-events`)

Produced by **PaymentService**. Key: `paymentId` or `orderId`.

### PAYMENT_SUCCESSFUL

```json
{
  "event_id": "c0c3e089-27e5-6f65-b0a6-h9d77h3h2c9e",
  "event_type": "PAYMENT_SUCCESSFUL",
  "timestamp": 1724684415,
  "payment_id": "PAY-2001",
  "order_id": "ORD-1001",
  "amount": 159.48,
  "currency": "USD",
  "method": "CARD",
  "status": "SUCCESS"
}
```

### PAYMENT_FAILED

```json
{
  "event_id": "d1d4f19a-38f6-7g76-c1b7-i0e88i4i3d0f",
  "event_type": "PAYMENT_FAILED",
  "timestamp": 1724684420,
  "payment_id": "PAY-2002",
  "order_id": "ORD-1002",
  "amount": 49.99,
  "error_code": "CARD_DECLINED",
  "status": "FAILED"
}
```

---

## Flow

1. **OrderService** publishes `ORDER_CREATED` (and later `ORDER_CANCELLED` if needed) to `order-events`.
2. **PaymentService** publishes `PAYMENT_SUCCESSFUL` or `PAYMENT_FAILED` to `payment-events`.
3. **NotificationService** consumes from both topics and sends notifications (e.g. email/SMS) for each event.
4. **MirrorMaker 2** replicates `order-events` and `payment-events` to the DR cluster as `primary.order-events` and `primary.payment-events`, so a standby NotificationService can run against DR after a failover.
