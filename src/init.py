from jobs import TopicToLakeJob, MaxSubflowJob

jobs = [TopicToLakeJob(), MaxSubflowJob(slide_ms=200)]
for job in jobs:
    job.send()