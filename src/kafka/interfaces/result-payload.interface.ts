export interface ClassificationResult {
  predictedClass: string;
  confidence: number;
  probabilities: {
    Elliptical: number;
    Spiral: number;
    Barred_Spiral: number;
    Edge_on: number;
    Irregular_Merger: number;
  };
}

export interface ResultPayload {
  jobId: string;
  clientId: string;
  imageKey: string;
  status: 'SUCCESS' | 'ERROR';
  classification?: ClassificationResult;
  error?: string;
}
