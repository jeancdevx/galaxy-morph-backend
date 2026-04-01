import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Consumer } from 'kafkajs';
import { KafkaService } from './kafka.service.js';
import { ResultPayload } from './interfaces/result-payload.interface.js';
import { NotificationsGateway } from '../notifications/notifications.gateway.js';

@Injectable()
export class ConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ConsumerService.name);
  private consumer: Consumer;

  private classificationsService: any;

  constructor(
    private readonly configService: ConfigService,
    private readonly kafkaService: KafkaService,
    private readonly notificationsGateway: NotificationsGateway,
  ) {}

  /**
   * Set ClassificationsService reference (avoids circular dependency).
   * Called from ClassificationsModule.onModuleInit()
   */
  setClassificationsService(service: any) {
    this.classificationsService = service;
  }

  async onModuleInit() {
    const kafka = this.kafkaService.getKafkaInstance();
    const groupId = this.configService.get<string>('kafka.consumerGroupId')!;
    const resultsTopic = this.configService.get<string>(
      'kafka.topics.results',
    )!;

    this.consumer = kafka.consumer({ groupId });

    try {
      await this.consumer.connect();
      await this.consumer.subscribe({
        topic: resultsTopic,
        fromBeginning: false,
      });

      await this.consumer.run({
        eachMessage: async ({ message }) => {
          try {
            const value = message.value?.toString();
            if (!value) return;

            const payload: ResultPayload = JSON.parse(value);
            this.logger.debug(
              `Received result for job ${payload.jobId}, image ${payload.imageKey}`,
            );

            // Update in-memory job store
            if (this.classificationsService) {
              this.classificationsService.handleResult(payload);
            }

            // Notify client via WebSocket
            if (payload.status === 'SUCCESS') {
              this.notificationsGateway.notifyClient(
                payload.clientId,
                'classification:result',
                {
                  jobId: payload.jobId,
                  imageKey: payload.imageKey,
                  ...payload.classification,
                },
              );
            } else {
              this.notificationsGateway.notifyClient(
                payload.clientId,
                'classification:error',
                {
                  jobId: payload.jobId,
                  imageKey: payload.imageKey,
                  error: payload.error,
                },
              );
            }
          } catch (error) {
            this.logger.error(`Error processing result message: ${error}`);
          }
        },
      });

      this.logger.log(
        `Kafka consumer subscribed to "${resultsTopic}" (group: ${groupId})`,
      );
    } catch (error) {
      this.logger.warn(
        `Kafka consumer connection failed — results won't be consumed. Error: ${error}`,
      );
    }
  }

  async onModuleDestroy() {
    try {
      await this.consumer?.disconnect();
      this.logger.log('Kafka consumer disconnected');
    } catch (error) {
      this.logger.error(`Error disconnecting Kafka consumer: ${error}`);
    }
  }
}
