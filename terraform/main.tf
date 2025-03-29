terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.27.0"
    }
  }
}


provider "google" {
  credentials = file(var.credentials)
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "static" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
}

resource "google_bigquery_dataset" "demo_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

resource "google_composer_environment" "airflow" {
  name   = "your-environment-name"
  region = var.region

  config {
    # Add the software_config block here
    software_config {
      image_version = "composer-2.4.3-airflow-2.5.1"  # Example version
      
      env_variables = {
        AIRFLOW_VAR_GCP_PROJECT   = var.project_id
        AIRFLOW_VAR_GCS_BUCKET    = google_storage_bucket.static.name
        AIRFLOW_VAR_BQ_DATASET    = google_bigquery_dataset.demo_dataset.dataset_id
        AIRFLOW_VAR_GCP_REGION    = var.region
      }
    }

    # Keep your existing node configuration
    node_config {
      service_account = google_service_account.airflow_sa.email
      machine_type    = "n1-standard-2"
      disk_size_gb    = 30
    }
  }

  depends_on = [
    google_project_service.required_apis,
    google_service_account.airflow_sa
  ]
}