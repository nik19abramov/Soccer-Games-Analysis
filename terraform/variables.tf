variable "credentials" {
  description = "My Credentials"
  default     = "../google/credentials.json"
}

variable "project_id" {
  description = "Project"
  default     = "ny-rides-nikolay"
}

variable "region" {
  description = "Region"
  default     = "europe-west1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "EU"
}

variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  default     = "soccer"
}

variable "gcs_bucket_name" {
  description = "Google Cloud Bucket Name"
  default     = "dezoomcamp_hw3_2025_1"
}
