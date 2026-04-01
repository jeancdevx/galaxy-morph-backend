import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { randomUUID } from 'node:crypto';
import { KafkaService } from '../kafka/kafka.service.js';
import { JobStatus } from '../kafka/enums/job-status.enum.js';
import { IngestionPayload } from '../kafka/interfaces/ingestion-payload.interface.js';
import {
  ResultPayload,
  ClassificationResult,
} from '../kafka/interfaces/result-payload.interface.js';
import { StartClassificationDto } from './dtos/start-classification.dto.js';
import { NotificationsGateway } from '../notifications/notifications.gateway.js';

export interface ImageResult {
  imageKey: string;
  status: 'PENDING' | 'SUCCESS' | 'ERROR';
  classification?: ClassificationResult;
  error?: string;
}

export interface Job {
  jobId: string;
  clientId: string;
  status: JobStatus;
  totalImages: number;
  processedImages: number;
  results: ImageResult[];
  createdAt: string;
  completedAt?: string;
}

@Injectable()
export class ClassificationsService {
  private readonly logger = new Logger(ClassificationsService.name);

  /** In-memory job store */
  private readonly jobs = new Map<string, Job>();

  private readonly ingestionTopic: string;

  constructor(
    private readonly kafkaService: KafkaService,
    private readonly configService: ConfigService,
    private readonly notificationsGateway: NotificationsGateway,
  ) {
    this.ingestionTopic = this.configService.get<string>(
      'kafka.topics.ingestion',
    )!;
  }

  async startClassification(dto: StartClassificationDto) {
    const jobId = randomUUID();
    const timestamp = new Date().toISOString();

    // Create job in store
    const job: Job = {
      jobId,
      clientId: dto.clientId,
      status: JobStatus.QUEUED,
      totalImages: dto.images.length,
      processedImages: 0,
      results: dto.images.map((img) => ({
        imageKey: img.key,
        status: 'PENDING',
      })),
      createdAt: timestamp,
    };

    this.jobs.set(jobId, job);

    // Publish one message per image to Kafka
    const messages = dto.images.map((img) => {
      const payload: IngestionPayload = {
        jobId,
        imageKey: img.key,
        clientId: dto.clientId,
        timestamp,
      };

      return {
        key: jobId,
        value: JSON.stringify(payload),
      };
    });

    try {
      await this.kafkaService.emit(this.ingestionTopic, messages);
      this.logger.log(
        `Job ${jobId} queued: ${dto.images.length} image(s) → ${this.ingestionTopic}`,
      );
    } catch (error) {
      job.status = JobStatus.FAILED;
      this.logger.error(`Failed to publish job ${jobId}: ${error}`);
      throw error;
    }

    return {
      jobId,
      status: job.status,
      count: job.totalImages,
    };
  }

  getJob(jobId: string) {
    const job = this.jobs.get(jobId);
    if (!job) {
      throw new NotFoundException(`Job "${jobId}" not found`);
    }
    return job;
  }

  listJobs(page = 1, limit = 20) {
    const allJobs = Array.from(this.jobs.values())
      .sort(
        (a, b) =>
          new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime(),
      );

    const start = (page - 1) * limit;
    const items = allJobs.slice(start, start + limit);

    return {
      items,
      total: allJobs.length,
      page,
      limit,
      totalPages: Math.ceil(allJobs.length / limit),
    };
  }

  /**
   * Handle a result from Spark via Kafka consumer.
   */
  handleResult(payload: ResultPayload) {
    const job = this.jobs.get(payload.jobId);
    if (!job) {
      this.logger.warn(
        `Received result for unknown job: ${payload.jobId}`,
      );
      return;
    }

    // Update image result
    const imageResult = job.results.find(
      (r) => r.imageKey === payload.imageKey,
    );
    if (imageResult) {
      imageResult.status = payload.status === 'SUCCESS' ? 'SUCCESS' : 'ERROR';
      imageResult.classification = payload.classification;
      imageResult.error = payload.error;
    }

    job.processedImages++;
    job.status = JobStatus.PROCESSING;

    // Check if all images are done
    if (job.processedImages >= job.totalImages) {
      const hasErrors = job.results.some((r) => r.status === 'ERROR');
      job.status = hasErrors ? JobStatus.FAILED : JobStatus.COMPLETE;
      job.completedAt = new Date().toISOString();

      // Notify client that the entire job is complete
      this.notificationsGateway.notifyClient(
        job.clientId,
        'classification:complete',
        {
          jobId: job.jobId,
          status: job.status,
          totalProcessed: job.processedImages,
        },
      );

      this.logger.log(
        `Job ${job.jobId} complete: ${job.processedImages}/${job.totalImages} images (status: ${job.status})`,
      );
    }
  }
}
