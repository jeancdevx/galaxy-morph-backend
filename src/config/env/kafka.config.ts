import { registerAs } from '@nestjs/config';

export default registerAs('kafka', () => ({
  brokers: (process.env.KAFKA_BROKERS ?? 'kafka-1:9092,kafka-2:9092,kafka-3:9092').split(','),
  clientId: process.env.KAFKA_CLIENT_ID ?? 'galaxy-morph-api',
  consumerGroupId: process.env.KAFKA_CONSUMER_GROUP_ID ?? 'galaxy-morph-api-group',
  topics: {
    ingestion: process.env.KAFKA_INGESTION_TOPIC ?? 'galaxy.ingestion',
    results: process.env.KAFKA_RESULTS_TOPIC ?? 'galaxy.results',
  },
}));
