import sys
import jobs


def run_jobs(job_name):
    jobs = {'JOB01': 'daily_extractions.py',
            'JOB02': 'weekly_financial_statement.py',
            'JOB03': 'weekly_yahoo_conesus.py',
            'JOB04': 'weekly_price_job.py',
            'JOB05': 'weekly_factor_job.py'}

    if job_name in jobs.keys():
        jobs[job_name]
    else:
        pass