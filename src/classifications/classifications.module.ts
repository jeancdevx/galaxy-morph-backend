import { Module, OnModuleInit } from '@nestjs/common';
import { ClassificationsController } from './classifications.controller.js';
import { ClassificationsService } from './classifications.service.js';
import { KafkaModule } from '../kafka/kafka.module.js';
import { ConsumerService } from '../kafka/consumer.service.js';
import { NotificationsModule } from '../notifications/notifications.module.js';

@Module({
  imports: [KafkaModule, NotificationsModule],
  controllers: [ClassificationsController],
  providers: [ClassificationsService],
})
export class ClassificationsModule implements OnModuleInit {
  constructor(
    private readonly classificationsService: ClassificationsService,
    private readonly consumerService: ConsumerService,
  ) {}

  onModuleInit() {
    // Wire up the circular dependency: ConsumerService needs ClassificationsService
    this.consumerService.setClassificationsService(
      this.classificationsService,
    );
  }
}
