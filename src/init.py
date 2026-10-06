from jobs import TopicToLakeJob, MaxSubflowJob

jobs = [TopicToLakeJob(), MaxSubflowJob()]
for job in jobs:
    job.send()