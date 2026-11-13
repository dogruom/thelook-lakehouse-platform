output "database_names" {
  description = "Names of all Glue databases managed by this module (sorted)."
  value = sort(concat(
    [for db in aws_glue_catalog_database.spark : db.name],
    [for db in aws_glue_catalog_database.athena : db.name],
  ))
}

output "athena_workgroup" {
  description = "Name of the Athena workgroup."
  value       = aws_athena_workgroup.main.name
}
