output "job_names" {
  description = "Map of pipeline job key to Glue job name."
  value       = { for job, res in aws_glue_job.pipeline : job => res.name }
}
