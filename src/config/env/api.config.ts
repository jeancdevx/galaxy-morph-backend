import { registerAs } from '@nestjs/config';

export default registerAs('api', () => ({
  port: parseInt(process.env.API_PORT ?? '3001', 10),
  corsOrigins: process.env.CORS_ORIGINS ?? 'http://localhost:3000',
}));
