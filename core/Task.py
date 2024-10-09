from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime


class Task:
    def __init__(self,id,schedule,active=None,steps=None,force_build=None,code=None,type=None,pipeline=None):
        self.id = id
        self.schedule = schedule if schedule else ""
        self.active = True if active=='true' else False
        self.steps = steps
        self.force_build = True if force_build=='true' else False
        self.type = type
        self.pipeline=pipeline
    def start(self):
        scheduler = BlockingScheduler()
        print(f"Starting Task {self.id}\nSchedule: {self.schedule}")
        print(CronTrigger.from_crontab(self.schedule))
        scheduler.add_job(self.pipeline.run, CronTrigger.from_crontab(self.schedule))
        scheduler.start()
        # next_run_time=scheduler.next_run_time
        # print(f"Next Scheduled Run At: {next_run_time}")
        # while True:
        #     if scheduler.next_run_time !=next_run_time:
        #         next_run_time=scheduler.next_run_time
        #         print(f"Next Scheduled Run At: {next_run_time}")
        #     pass
