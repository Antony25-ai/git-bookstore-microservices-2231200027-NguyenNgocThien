import express from 'express';
import axios from 'axios';
import db from './db.js';
import { connectToBroker, publishMessage } from './broker.js';

const app = express();
app.use(express.json());

// RabbitMQ
connectToBroker().catch(err => console.error('Broker init error', err));

// Create order
app.post('/', async (req, res) => {
  try {
    const productId = Number(req.body.productId);
    const quantity = Number(req.body.quantity);
    if (!Number.isInteger(productId)) return res.status(400).json({ error: 'productId required' });
    if (!Number.isInteger(quantity) || quantity <= 0) return res.status(400).json({ error: 'quantity must be a positive integer' });

    let product;
    try {
      const baseUrl = process.env.PRODUCT_SERVICE_URL || 'http://product-service:8002';
      ({ data: product } = await axios.get(`${baseUrl}/${productId}`, { timeout: 3000 }));
    } catch (err) {
      if (err.response?.status === 404) return res.status(404).json({ error: 'Product not found' });
      console.error('Product service error:', err.message);
      return res.status(503).json({ error: 'Product service unavailable' });
    }

    const r = await db.query(
      'INSERT INTO orders (product_id, quantity, status) VALUES ($1, $2, $3) RETURNING *',
      [productId, quantity, 'PENDING']
    );
    const order = r.rows[0];

    const event = {
      event: 'ORDER_CREATED',
      orderId: order.id,
      product: { id: product.id, title: product.title, price: product.price },
      quantity: order.quantity,
      status: order.status,
      createdAt: order.created_at
    };
    await publishMessage('order.created', event);
    console.log('Published ORDER_CREATED event to RabbitMQ queue "order.created":', JSON.stringify(event));

    res.status(201).json({ ...order, product });
  } catch (err) {
    console.error('Create order error:', err);
    res.status(500).json({ error: 'Internal server error' });
  }
});

// List orders
app.get('/', async (_req, res) => {
  const r = await db.query('SELECT * FROM orders ORDER BY id DESC');
  res.json(r.rows);
});

// Get order by id
app.get('/:id', async (req, res) => {
  const id = Number(req.params.id);
  const r = await db.query('SELECT * FROM orders WHERE id = $1', [id]);
  if (r.rows.length === 0) return res.status(404).json({ error: 'Order not found' });
  res.json(r.rows[0]);
});

const PORT = 8003;
app.listen(PORT, () => console.log(`Order Service running on ${PORT}`));
