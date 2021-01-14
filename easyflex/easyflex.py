from easyflex.dataservices.import_modules import query_all
from easyflex.functions import cleanup
from easyflex.run_settings import set_run_parameters
import logging


def query(
    modules=None,
    incremental=False,
    day_offset=7,
    years=None,
    pre_cleanup=False,
    post_cleanup=True,
    debug=False,
):

    run_params = set_run_parameters()

    if pre_cleanup:
        cleanup(run_params.pickledir, ext=".pkl")
    query_all(
        run_params,
        modules=modules,
        incremental=incremental,
        day_offset=day_offset,
        years=years,
        debug=debug,
    )
    if post_cleanup:
        cleanup(run_params.stagingdir, ext=".csv")


if __name__ == "__main__":

    logging.info("start of script!")
    query(modules=["ds_wm_medewerkers"], years=[2018, 2019, 2020])
    logging.info("end of script!")
