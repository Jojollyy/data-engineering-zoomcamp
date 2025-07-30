variable "credentials" {
    description = "GCS bucket credentials"
    default     = "./keys/mycreds.json"
}

variable "project" {
    description = "GCS bucket project"
    default     = "notional-cirrus-467301-j2"
}

variable "region" {
    description = "GCS bucket region"
    default     = "us-central1"
}

variable "location" {
    description = "GCS bucket location"
    default     = "US"
}

variable "bq_dataset_name" {
    description = "My BigQuery dataset name"
    default     = "demo_dataset"
}

variable "gcs_bucket_name" {
    description = "GCS bucket name"
    default     = "terraform-demo-bucket-467301-j2"
}

variable "gcs_storage_class" {
    description = "Bucket storage class"
    default     = "STANDARD"
}