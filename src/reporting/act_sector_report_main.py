from datetime import datetime
from core.infra.logging_config import setup_logging, get_logger
from core.reporting.reporting_io import xlsx_sheets_io
from reporting.act_sector_report import generate_act_sectors_reports

logger = get_logger(__name__)

if __name__ == "__main__":
    setup_logging()
    # gp_ids = [44941, 44420, 44738, 41430, 50805, 45450, 50613, 50961, 55236, 51901, 45062, 51036, 50809, 53176, 42243,
    #           40642, 53179, 49318, 50716, 53975, 50824, 47923, 44777, 45144, 43595, 42108, 45395, 48360, 50717, 48672,
    #           40644, 48665, 41151, 41089, 57167, 48457, 50833, 49317, 25765,26961,28349,29527,34095,34673,36020,38955,43820,44240,46111,48761,50086,50805,51517,53414]
    # gp_ids = [44420]
    gp_ids = [44420,42108,42243,45450,44941,50809,47923,50824,48043,41089,41082,48761,40642,41151,53176,53179,53414,55236,44240,45144,45395,46111,51036,43416,50086,43595,43180,43820]

    gp_ids = gp_ids[-5:]
    act_sector_reports = generate_act_sectors_reports(gp_ids)

    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"query_diags_{current_datetime}.xlsx"
    logger.info(f"Writing to file: {filename}")
    xlsx_sheets_io(act_sector_reports, filename)