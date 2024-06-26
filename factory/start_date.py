from croniter import croniter
from datetime import datetime, tzinfo

import pendulum

class StartDate:
    """Creates an object with date information for DAGs."""
    def __init__(self, crontab=None, TZ='America/Sao_Paulo'):
        self.current_datetime = pendulum.now(TZ)
        if crontab:
            self.cron_iter = croniter(crontab,self.current_datetime)
            # Goes back 2 times because of the Airflow lag.
            # First time gets the time this run started, second time gets the Airflow start date.
            self.processing_datetime = self.cron_iter.get_prev(datetime)
            self.processing_datetime = self.cron_iter.get_prev(datetime)
        else:
            self.processing_datetime = self.current_datetime
        
        self._day = self.processing_datetime.strftime('%d')
        self._month = self.processing_datetime.strftime('%m')
        self._year = self.processing_datetime.strftime('%Y')
        self._TZ = TZ
        # Values are hardcoded this way in all DAGs, so I kept it.
        self.start_date = datetime(
            year=2021,
            month=1,
            day=1,
            hour=0,
            minute=0,
            tzinfo=pendulum.timezone(TZ)
        )

    def get_start_date(self) -> datetime:
        return self.start_date

    def get_day(self) -> str:
        return self._day

    def get_month(self) -> str:
        return self._month

    def get_year(self) -> str:
        return self._year

    def get_current_datetime(self) -> datetime:
        return self.current_datetime

    def get_processing_datetime(self) -> datetime:
        return self.processing_datetime
    
    def get_timezone(self) -> tzinfo:
        return pendulum.timezone(self._TZ)
