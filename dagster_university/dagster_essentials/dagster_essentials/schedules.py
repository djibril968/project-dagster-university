import dagster as dg
from dagster_essentials.jobs import trip_update_job, weekly_update_job


trip_update_schedule = dg.ScheduleDefinition(
    job = trip_update_job,
    cron_schedule= "0 4 * * 1",
    name = "trip_update_schedule"
)


weekly_update_schedule = dg.ScheduleDefinition(
    job = weekly_update_job,
    cron_schedule= "0 0 * * 1",
    name = "weekly_update_schedule"
)
