import { Module, forwardRef } from '@nestjs/common';
import { KafkaService } from './kafka.service.js';
import { ConsumerService } from './consumer.service.js';
import { NotificationsModule } from '../notifications/notifications.module.js';

@Module({
  imports: [forwardRef(() => NotificationsModule)],
  providers: [KafkaService, ConsumerService],
  exports: [KafkaService, ConsumerService],
})
export class KafkaModule {}
