# ============================================================================
# DATAPROC MODULE — Serverless-friendly Spark for Silver transforms
# ============================================================================
# Uses a small, auto-scaling cluster that can scale to zero workers.
# Master: e2-standard-2 (2 vCPU, 8 GB) — eligible for free tier compute.
# Workers: preemptible e2-standard-2, min 0 / max 2, auto-delete after idle.
# ============================================================================

resource "google_dataproc_cluster" "spark" {
  name    = "${var.project_name}-spark-${var.environment}"
  region  = var.region
  project = var.project_id

  graceful_decommission_timeout = "120s"

  labels = {
    environment = var.environment
    project     = var.project_name
    layer       = "processing"
  }

  cluster_config {
    staging_bucket = var.staging_bucket

    # --- Master node (always on) -------------------------------------------
    master_config {
      num_instances = 1
      machine_type  = var.master_machine_type

      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
      }
    }

    # --- Worker nodes (preemptible, auto-scaling) --------------------------
    worker_config {
      num_instances  = var.min_workers
      machine_type   = var.worker_machine_type
      preemptibility = "PREEMPTIBLE"

      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
      }
    }

    # --- Auto-scaling: scale down to 0 workers when idle -------------------
    autoscaling_config {
      policy_uri = google_dataproc_autoscaling_policy.spark.name
    }

    # --- Software: Spark + Iceberg connector + GCS connector ---------------
    software_config {
      image_version = var.image_version

      override_properties = {
        "spark:spark.jars.packages"                           = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
        "spark:spark.sql.catalog.lakehouse"                   = "org.apache.iceberg.spark.SparkCatalog"
        "spark:spark.sql.catalog.lakehouse.type"              = "hadoop"
        "spark:spark.sql.catalog.lakehouse.warehouse"         = "gs://${var.silver_bucket_name}/"
        "spark:spark.sql.extensions"                          = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        "spark:spark.sql.adaptive.enabled"                    = "true"
        "spark:spark.sql.adaptive.coalescePartitions.enabled" = "true"
        "dataproc:dataproc.allow.zero.workers"                = "true"
      }
    }

    # --- GCE cluster config ------------------------------------------------
    gce_cluster_config {
      zone = "${var.region}-b"

      service_account        = var.service_account_email
      service_account_scopes = ["cloud-platform"]

      internal_ip_only = false

      metadata = {
        "enable-oslogin" = "true"
      }
    }

    # --- Auto-delete idle cluster (cost control) ---------------------------
    lifecycle_config {
      idle_delete_ttl = var.idle_delete_ttl
    }
  }
}

# --- Auto-scaling policy ---------------------------------------------------

resource "google_dataproc_autoscaling_policy" "spark" {
  policy_id = "${var.project_name}-spark-autoscale-${var.environment}"
  location  = var.region
  project   = var.project_id

  basic_algorithm {
    yarn_config {
      scale_up_factor               = 1.0
      scale_down_factor             = 1.0
      scale_up_min_worker_fraction  = 0.0
      graceful_decommission_timeout = "1h"
      cooldown_period               = "120s"
    }
  }

  worker_config {
    min_instances = var.min_workers
    max_instances = var.max_workers
    weight        = 1
  }

  secondary_worker_config {
    min_instances = 0
    max_instances = 0
    weight        = 0
  }
}
