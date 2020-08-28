# Chris Hsu
# Simulate covid case data and issues surrounding using Episode Date in a current window metric.

import logging
import random
from datetime import date, timedelta

import pandas as pd

logging.basicConfig(level=logging.INFO)


class DelayGenerator:
    min_test_result_delay = 0  # noninclusive
    max_test_result_delay = 0  # noninclusive
    mode_test_result_delay = 0

    min_reporting_delay = 0  # noninclusive
    max_reporting_delay = 0  # noninclusive
    mode_reporting_delay = 0

    pct_tested_with_symptoms = 0
    min_get_tested_delay = 0  # noninclusive
    max_get_tested_delay = 0  # noninclusive
    mode_get_tested_delay = 0

    def __init__(self, min_result_delay, max_result_delay, mode_result_delay):

        DelayGenerator.min_test_result_delay = min_result_delay
        DelayGenerator.max_test_result_delay = max_result_delay
        DelayGenerator.mode_test_result_delay = mode_result_delay

    def get_min_delay(self):
        return round(DelayGenerator.min_test_result_delay + DelayGenerator.min_reporting_delay)

    def get_max_delay(self):
        return round(
            DelayGenerator.max_get_tested_delay + DelayGenerator.max_test_result_delay + DelayGenerator.max_reporting_delay)

    def get_test_result_delay(self):

        return round(random.triangular(DelayGenerator.min_test_result_delay, DelayGenerator.max_test_result_delay,
                                       DelayGenerator.mode_test_result_delay))

    def get_reporting_delay(self):

        return round(random.triangular(DelayGenerator.min_reporting_delay, DelayGenerator.max_reporting_delay,
                                       DelayGenerator.mode_reporting_delay))

    def get_days_after_symptoms_tested(self):

        random_number = random.randint(1, 100)
        if random_number <= DelayGenerator.pct_tested_with_symptoms:
            return round(random.triangular(DelayGenerator.min_get_tested_delay, DelayGenerator.max_get_tested_delay,
                                           DelayGenerator.mode_get_tested_delay))
        else:
            return None


class CaseGenerator:

    def __init__(self, delay_gen):
        pass

    @staticmethod
    def create_case(delay_gen, sample_date):

        symptom_preceded = delay_gen.get_days_after_symptoms_tested()
        if symptom_preceded:
            symptom_date = sample_date - timedelta(days=symptom_preceded)
        else:
            symptom_date = None
        lab_result_date = sample_date + timedelta(days=delay_gen.get_test_result_delay())
        report_to_state_date = lab_result_date + timedelta(days=delay_gen.get_reporting_delay())

        new_case = CovidCase(symptom_date, sample_date, lab_result_date, report_to_state_date)

        return new_case


class BadDateError(Exception):
    pass


class NullDateError(Exception):
    pass


class CovidCase:
    symptom_date = None
    sample_date = None
    lab_result_date = None
    report_to_state_date = None

    def __init__(self, sym_date, samp_date, lab_date, rep_date):

        if samp_date is None or lab_date is None or rep_date is None:
            raise NullDateError

        if lab_date < samp_date:
            raise BadDateError

        if rep_date < lab_date:
            raise BadDateError

        self.symptom_date = sym_date
        self.sample_date = samp_date
        self.lab_result_date = lab_date
        self.report_to_state_date = rep_date

    def episode_date(self):
        if self.symptom_date:
            return min(self.symptom_date, self.sample_date)
        else:
            return self.sample_date

    def reporting_date(self):
        return self.report_to_state_date

    def to_list(self):
        return [self.symptom_date, self.sample_date, self.lab_result_date, self.report_to_state_date,
                self.episode_date()]

    def display(self):
        logging.info("Symptoms: {0}".format(self.symptom_date))
        logging.info("Sample Date: {0}".format(self.sample_date))
        logging.info("Lab Result: {0}".format(self.lab_result_date))
        logging.info("Report to State: {0}".format(self.report_to_state_date))
        logging.info("___________________________________")


def build_backdata(delay_gen, reporting_date, reporting_lag, number_of_cases_per_day, sliding_window_size):
    myCg = CaseGenerator(delay_gen)

    logging.info("We are reporting as of: {0}".format(reporting_date))
    start_generating_cases_date = (reporting_date - timedelta(days=sliding_window_size + reporting_lag + delay_gen.get_max_delay() + 1))
    logging.debug("start case generation: {0}".format(start_generating_cases_date))

    all_cases = []
    logging.info(
        "Given the max delay of {0} from episode date until reporting date, we will generate cases starting:{1}".format(
            delay_gen.get_max_delay(), start_generating_cases_date))
    for day in range((reporting_date - start_generating_cases_date).days + 2):

        active_date = start_generating_cases_date + timedelta(days=day)
        for case in range(number_of_cases_per_day):
            all_cases.append(myCg.create_case(delay_gen, active_date).to_list())

    logging.info("Done generating cases.")

    return pd.DataFrame(data=all_cases)


def filter_dataframe_episode(bucket_df, window_end_date, report_date, window_size):
    # logging.info("build_date:{0}".format(date_of_data_build))
    # logging.info("report_date:{0}".format(report_date))

    has_it_been_reported_filter = bucket_df["report_to_state_date"] <= report_date
    filtered_df = bucket_df.where(has_it_been_reported_filter, inplace=False)

    filter3 = filtered_df["episode_date"] > (window_end_date - timedelta(days=window_size))
    filtered_df.where(filter3, inplace=True)

    filter4 = filtered_df["episode_date"] <= (window_end_date)
    filtered_df.where(filter4, inplace=True)

    # logging.info(filtered_df)

    return filtered_df


def filter_dataframe_report(bucket_df, date_of_data_build, report_date, window_size):
    has_it_been_reported_filter = bucket_df["report_to_state_date"] <= report_date
    filtered_df = bucket_df.where(has_it_been_reported_filter, inplace=False)

    filter3 = filtered_df["report_to_state_date"] > (date_of_data_build - timedelta(days=window_size))
    filtered_df.where(filter3, inplace=True)

    filter4 = filtered_df["report_to_state_date"] <= (date_of_data_build)
    filtered_df.where(filter4, inplace=True)

    return filtered_df


def generate_stats(dg, sliding_window_size, daily_cases, reporting_lag):
    # for now just generating data to support 1 days report.
    report_date = date(2020, 8, 8)


    bucket_df = build_backdata(delay_gen=dg,
                               reporting_date=report_date,
                               reporting_lag=reporting_lag,
                               number_of_cases_per_day=daily_cases,
                               sliding_window_size=sliding_window_size)
    # set column headers on dataframe
    bucket_df.columns = ['symptom_date', 'sample_date', 'lab_result_date', 'report_to_state_date', 'episode_date']
    # get dataframe for a given reporting date.

    window_end_date = report_date - timedelta(days=reporting_lag)

    # data filter
    # get records reported to the state before or on the report date?
    output_df = filter_dataframe_episode(bucket_df, window_end_date, report_date, sliding_window_size)
    lag_episode_date_count = output_df.count()[1]

    output_df = filter_dataframe_report(bucket_df, report_date, report_date, sliding_window_size)
    rep_date_count = output_df.count()[1]

    return rep_date_count, lag_episode_date_count


###############################################################################################
def run_job():
    results = []
    reporting_lag = 7
    window_size = 7
    daily_cases = 2000

    for x in range(21):
        dg = DelayGenerator(0, 2 * x, x)
        rep_date_count, episode_date_count = generate_stats(dg, window_size, daily_cases, reporting_lag)

        logging.debug("Cases Based on Reporting Date:{0}".format(rep_date_count))
        logging.debug("Cases Based on Episode Date:{0}".format(episode_date_count))

        logging.debug("********************************")

        results.append([x, rep_date_count, episode_date_count])

    result_df = pd.DataFrame(results)

    logging.info(result_df)


if __name__ == '__main__':
    pd.set_option('display.max_rows', None)
    run_job()
