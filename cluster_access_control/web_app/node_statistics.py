import datetime
from typing import Final

from fastapi import APIRouter, HTTPException

from cluster_access_control.database_usage_statistics.postgres_handling import PostgresHandler


class NodeStatistics:
    DAYS_IN_WEEK: Final[int] = 7

    def __init__(self, postgres_handler: PostgresHandler):
        self.router = APIRouter()
        self._postgres_handler = postgres_handler
        self.router.add_api_route(
            "/api/v1/node_survival_chance/{node_name}/{time_range_in_minutes}", self.node_survival_chance,
            methods=["GET"]
        )

    def _get_check_in_time_single_day(self, node_name: str, start_time: datetime.datetime.timestamp,
                                      time_range_in_minutes: int) -> dict[int, list]:
        # node was registered more than a week ago
        if (start_time + datetime.timedelta(minutes=time_range_in_minutes)).day != start_time.day:
            # current day end
            next_day = (start_time + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
            end_of_day = next_day - datetime.timedelta(seconds=1)
            check_in_times = self._postgres_handler.get_node_check_in_times(node_name, start_time.day,
                                                                            start_time.day,
                                                                            PostgresHandler.get_seconds_since_midnight(
                                                                                start_time),
                                                                            PostgresHandler.get_seconds_since_midnight(
                                                                                end_of_day
                                                                            ))
            minutes_from_next_day = (end_of_day - start_time).seconds // 60  # minutes calculated from current day
            # next day
            if minutes_from_next_day > 0:
                check_in_times.update(self._postgres_handler.get_node_check_in_times(node_name, start_time.day,
                                                                                     start_time.day,
                                                                                     PostgresHandler.get_seconds_since_midnight(
                                                                                         next_day),
                                                                                     PostgresHandler.get_seconds_since_midnight(
                                                                                         next_day + datetime.timedelta(
                                                                                             minutes=time_range_in_minutes - minutes_from_next_day)
                                                                                     )))
        else:
            end_time = start_time + datetime.timedelta(minutes=time_range_in_minutes)
            check_in_times = self._postgres_handler.get_node_check_in_times(node_name, start_time.day,
                                                                            start_time.day,
                                                                            PostgresHandler.get_seconds_since_midnight(
                                                                                start_time),
                                                                            PostgresHandler.get_seconds_since_midnight(
                                                                                end_time))
        return check_in_times

    def node_survival_chance(self, node_name: str, time_range_in_minutes: int) -> float:
        if time_range_in_minutes >= 1440:
            raise HTTPException(status_code=400)

        if not self._postgres_handler.node_registered(node_name):
            raise HTTPException(status_code=404)
        try:
            node_registration_time = self._postgres_handler.get_node_registration_time(node_name)
            start_time = datetime.datetime.utcnow()
            days_since_registration = (start_time - node_registration_time).days
            if days_since_registration >= NodeStatistics.DAYS_IN_WEEK:
                check_in_times = self._get_check_in_time_single_day(node_name, start_time, time_range_in_minutes)

            elif days_since_registration >= 1:
                # we want the time of all previous days
                start_time = node_registration_time
                end_time = start_time + datetime.timedelta(days=time_range_in_minutes)
                # TODO: me

            else:
                end_time = start_time + datetime.timedelta(minutes=time_range_in_minutes)
                check_in_times = self._postgres_handler.get_node_check_in_times(node_name, start_time.day,
                                                                                start_time.day,
                                                                                PostgresHandler.get_seconds_since_midnight(
                                                                                    start_time),
                                                                                PostgresHandler.get_seconds_since_midnight(
                                                                                    end_time))

        except RuntimeWarning:
            raise HTTPException(status_code=400)

