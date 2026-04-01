import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';

import apiConfig from './env/api.config.js';
import kafkaConfig from './env/kafka.config.js';
import storageConfig from './env/storage.config.js';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: '.env',
      load: [apiConfig, kafkaConfig, storageConfig],
    }),
  ],
})
export class AppConfigModule {}
