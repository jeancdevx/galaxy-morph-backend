import { Module } from '@nestjs/common';
import { AppConfigModule } from './config/config.module.js';
import { StorageModule } from './storage/storage.module.js';
import { UploadsModule } from './uploads/uploads.module.js';
import { KafkaModule } from './kafka/kafka.module.js';
import { ClassificationsModule } from './classifications/classifications.module.js';
import { NotificationsModule } from './notifications/notifications.module.js';

@Module({
  imports: [
    AppConfigModule,
    StorageModule,
    UploadsModule,
    KafkaModule,
    ClassificationsModule,
    NotificationsModule,
  ],
})
export class AppModule {}
