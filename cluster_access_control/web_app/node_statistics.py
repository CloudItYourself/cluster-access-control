import datetime
from statistics import mean
from typing import Final, Optional

from fastapi import APIRouter, HTTPException
from fastapi_cache.decorator import cache

from cluster_access_control.database_usage_statistics.postgres_handling import (
    PostgresHandler,
)


class NodeStatistics:
    DAYS_IN_WEEK: Final[int] = 7
    SECONDS_IN_HOUR: Final[int] = 3600

    def __init__(self, postgres_handler: PostgresHandler):
        self.router = APIRouter()
        self._postgres_handler = postgres_handler
        self.router.add_api_route(
            "/api/v1/node_survival_chance/{node_name}/{time_range_in_minutes}",
            self.node_survival_chance,
            methods=["GET"],
        )
        self.router.add_api_route(
            "/api/v1/abrupt_disconnects/{node_name}",
            self.get_abrupt_disconnect_count,
            methods=["GET"],
        )

    def _get_check_in_time_single_day(
        self,
        node_name: str,
        start_day: int,
        end_day: int,
        start_time: datetime.datetime.timestamp,
        time_range_in_minutes: int,
    ) -> dict[int, list]:
        # node was registered more than a week ago
        if (
            start_time + datetime.timedelta(minutes=time_range_in_minutes)
        ).day != start_time.day:
            # current day end
            next_day = (start_time + datetime.timedelta(days=1)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            end_of_day = next_day - datetime.timedelta(seconds=1)
            check_in_times = self._postgres_handler.get_node_check_in_times(
                node_name,
                start_day,
                end_day,
                start_time,
                end_of_day,
            )
            minutes_from_next_day = (
                end_of_day - start_time
            ).seconds // 60  # minutes calculated from current day
            # next day
            if minutes_from_next_day > 0:
                check_in_times.update(
                    self._postgres_handler.get_node_check_in_times(
                        node_name,
                        (start_day + 1) % self.DAYS_IN_WEEK,
                        (end_day + 1) % self.DAYS_IN_WEEK,
                        next_day,
                        next_day
                        + datetime.timedelta(
                            minutes=time_range_in_minutes - minutes_from_next_day
                        ),
                    )
                )
        else:
            end_time = start_time + datetime.timedelta(minutes=time_range_in_minutes)
            check_in_times = self._postgres_handler.get_node_check_in_times(
                node_name,
                start_day,
                end_day,
                start_time,
                end_time,
            )
        return check_in_times

    def _get_check_in_times_and_expected_check_in_times(
        self, node_name: str, time_range_in_minutes: int
    ) -> float:
        try:
            node_registration_time = self._postgres_handler.get_node_registration_time(
                node_name
            )
            start_time = datetime.datetime.utcnow()
            days_since_registration = (start_time - node_registration_time).days

            if days_since_registration >= NodeStatistics.DAYS_IN_WEEK:
                check_in_times = self._get_check_in_time_single_day(
                    node_name,
                    start_time.weekday(),
                    start_time.weekday(),
                    start_time,
                    time_range_in_minutes,
                )
                expected_check_in_times = (
                    start_time - node_registration_time
                ).days // 7

                current_chance = 1.0
                for day, actual_checkins in check_in_times.items():
                    for check_in in actual_checkins[1:]: # we ignore the first value since this might be updated right now
                        if int(check_in[1]) > expected_check_in_times:
                            raise HTTPException(
                                status_code=500,
                                detail=f"check_in_count={int(check_in[1])} > expected_check_in_times={expected_check_in_times}",
                            )
                        current_chance *= int(check_in[1]) / expected_check_in_times
                return current_chance

            elif days_since_registration >= 1:
                # we want the time of all previous days
                start_day = node_registration_time.weekday()
                end_day = start_time.weekday()
                check_in_times = self._get_check_in_time_single_day(
                    node_name, start_day, end_day, start_time, time_range_in_minutes
                )
                expected_check_in_times = 1
                chances_dict = []
                for day, actual_checkins in check_in_times.items():
                    check_in_change_in_day = 1.0
                    for check_in in actual_checkins[1:]: # we ignore the first value since this might be updated right now
                        if int(check_in[1]) > expected_check_in_times:
                            raise HTTPException(
                                status_code=500,
                                detail=f"check_in_count={int(check_in[1])} > expected_check_in_times={expected_check_in_times}",
                            )
                        check_in_change_in_day *= (
                            int(check_in[1]) / expected_check_in_times
                        )
                    chances_dict.append(check_in_change_in_day)

                return mean(chances_dict)

            else:
                # too little time has passes...
                return 0.5
        except RuntimeWarning:
            raise HTTPException(status_code=400)

    def node_survival_change_internal_usage(
        self, node_name: str, time_range_in_minutes: int
    ) -> float:
        if time_range_in_minutes >= 1440:
            raise HTTPException(status_code=400)

        if not self._postgres_handler.node_registered(node_name):
            raise HTTPException(status_code=404)
        try:
            return self._get_check_in_times_and_expected_check_in_times(
                node_name, time_range_in_minutes
            )
        except RuntimeWarning:
            raise HTTPException(status_code=400)

    @cache(expire=30)
    def node_survival_chance(self, node_name: str, time_range_in_minutes: int) -> float:
        return self.node_survival_change_internal_usage(
            node_name, time_range_in_minutes
        )

    @cache(expire=60)
    def get_abrupt_disconnect_count(self, node_name: str) -> float:
        if not self._postgres_handler.node_registered(node_name):
            raise HTTPException(status_code=400)
        abruption_count = self._postgres_handler.get_abrupt_disconnect_for_node(
            node_name
        )
        if abruption_count == -1:
            raise HTTPException(
                status_code=500, detail="failed to get abrupt disconnect count"
            )
        node_registration_time = self._postgres_handler.get_node_registration_time(
            node_name
        )
        current_time = datetime.datetime.utcnow()
        if (
            current_time - datetime.timedelta(hours=abruption_count)
            < node_registration_time
        ):  # each abruption is a penalty of one hour
            return 0.0
        else:
            return max(
                0.0,
                1.0
                - (
                    abruption_count
                    * NodeStatistics.SECONDS_IN_HOUR
                    / (current_time - node_registration_time).total_seconds()
                ),
            )
