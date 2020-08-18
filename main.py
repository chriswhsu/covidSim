# Chris Hsu
# Simulate covid case data and issues surrounding using Episode Date in a current window metric.


from datetime import date, timedelta
import random
import logging
import pandas as pd

logging.basicConfig(level=logging.DEBUG)


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

    @staticmethod
    def get_min_delay():
        return round(DelayGenerator.min_test_result_delay + DelayGenerator.min_reporting_delay)

    @staticmethod
    def get_max_delay():
        return round(
            DelayGenerator.max_get_tested_delay + DelayGenerator.max_test_result_delay + DelayGenerator.max_reporting_delay)

    @staticmethod
    def get_test_result_delay():

        return round(random.triangular(DelayGenerator.min_test_result_delay, DelayGenerator.max_test_result_delay,
                                       DelayGenerator.mode_test_result_delay))

    @staticmethod
    def get_reporting_delay():

        return round(random.triangular(DelayGenerator.min_reporting_delay, DelayGenerator.max_reporting_delay,
                                       DelayGenerator.mode_reporting_delay))

    @staticmethod
    def get_days_after_symptoms_tested():

        random_number = random.randint(1, 100)
        if random_number <= DelayGenerator.pct_tested_with_symptoms:
            return round(random.triangular(DelayGenerator.min_get_tested_delay, DelayGenerator.max_get_tested_delay,
                                           DelayGenerator.mode_get_tested_delay))
        else:
            return None


class CaseGenerator:

    @staticmethod
    def create_case(sample_date):

        symptom_preceded = DelayGenerator.get_days_after_symptoms_tested()
        if symptom_preceded:
            symptom_date = sample_date - timedelta(days=symptom_preceded)
        else:
            symptom_date = None
        lab_result_date = sample_date + timedelta(days=DelayGenerator.get_test_result_delay())
        report_to_state_date = lab_result_date + timedelta(days=DelayGenerator.get_reporting_delay())

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
        return [self.symptom_date, self.sample_date, self.lab_result_date, self.report_to_state_date, self.episode_date()]

    def display(self):
        logging.info("Symptoms: {0}".format(self.symptom_date))
        logging.info("Sample Date: {0}".format(self.sample_date))
        logging.info("Lab Result: {0}".format(self.lab_result_date))
        logging.info("Report to State: {0}".format(self.report_to_state_date))
        logging.info("___________________________________")



def build_backdata(reporting_start_date, reporting_end_date, number_of_cases_per_day, sliding_window_size):
    logging.info("We will start reporting as of: {0}".format(reporting_start_date))
    start_generating_cases_date = (reporting_start_date - timedelta(days=sliding_window_size+1)) - timedelta(DelayGenerator.get_max_delay())
    logging.debug("start case generation: {0}".format(start_generating_cases_date))

    all_cases =[]
    logging.info(
        "Given the max delay of {0} from episode date until reporting date, we will generate cases starting:{1}".format(
            DelayGenerator.get_max_delay(), start_generating_cases_date))
    for x in range((reporting_end_date - start_generating_cases_date).days + 1):

        active_date = start_generating_cases_date + timedelta(days=x)
        for x in range(number_of_cases_per_day):
            all_cases.append(myCg.create_case(active_date).to_list())


    logging.info("Done generating cases.")

    return pd.DataFrame(data=all_cases)


###############################################################################################
if __name__ == '__main__':
    myCg = CaseGenerator()

    rep_start_date = date(2020, 8, 1)
    rep_end_date = date(2020, 8, 17)
    daily_cases = 10
    sliding_window_size = 14

    bucket_df = build_backdata(rep_start_date, rep_end_date, daily_cases, sliding_window_size)

    bucket_df.columns = ['symptom_date', 'sample_date', 'lab_result_date', 'report_to_state_date', 'episode_date']

    pd.set_option('display.max_rows', None)
    # logging.debug(bucket_df)

    # get dataframe for a given reporting date.
    # note that for a given reporting date report_to_state_date must be <= reporting date, or we don't have the data yet.


    active_date = date(2020,8,5)

    # data filter
    # was this reported to the state before the reporting date
    has_it_been_reported_filter = bucket_df["report_to_state_date"] <= active_date
    filtered_df = bucket_df.where(has_it_been_reported_filter, inplace=False)
    # logging.debug(filtered_df)

    time_window_filter = bucket_df["report_to_state_date"] > (active_date - timedelta(days=14))

    final_rd_filter_df = filtered_df.where(time_window_filter, inplace=False)
    logging.info("Cases Based on Reporting Date")
    logging.info(final_rd_filter_df.count())

    filter3 = bucket_df["episode_date"] > (active_date - timedelta(days=14))

    final_ed_filter_df = filtered_df.where(filter3, inplace=False)
    logging.info("Cases Based on Episode Date")

    logging.info(final_ed_filter_df.count())
    logging.info("********************************")


