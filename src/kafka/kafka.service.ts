import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Producer } from 'kafkajs';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaService.name);
  private kafka: Kafka;
  private producer: Producer;

  constructor(private readonly configService: ConfigService) {}

  async onModuleInit() {
    const brokers = this.configService.get<string[]>('kafka.brokers')!;
    const clientId = this.configService.get<string>('kafka.clientId')!;

    this.kafka = new Kafka({
      clientId,
      brokers,
      retry: {
        initialRetryTime: 300,
        retries: 10,
      },
    });

    this.producer = this.kafka.producer();

    try {
      await this.producer.connect();
      this.logger.log(`Kafka producer connected (brokers: ${brokers.join(', ')})`);
    } catch (error) {
      this.logger.warn(
        `Kafka producer connection failed — will retry on first message. Error: ${error}`,
      );
    }
  }

  async onModuleDestroy() {
    try {
      await this.producer?.disconnect();
      this.logger.log('Kafka producer disconnected');
    } catch (error) {
      this.logger.error(`Error disconnecting Kafka producer: ${error}`);
    }
  }

  async emit(topic: string, messages: { key?: string; value: string }[]) {
    await this.producer.send({
      topic,
      messages,
    });

    this.logger.debug(
      `Sent ${messages.length} message(s) to topic "${topic}"`,
    );
  }

  getKafkaInstance(): Kafka {
    return this.kafka;
  }
}
