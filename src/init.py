from jobs import TopicToLakeJob, MaxSubflowJob, MonitorMaxJob

jobs = [TopicToLakeJob(), MaxSubflowJob(slide_ms=200), MonitorMaxJob(threshold=1765, slide_ms=100)]
for job in jobs:
    job.send()