from easyflex.dataservices.import_modules import query_all
from easyflex.functions import cleanup, cat_modules
from easyflex.run_settings import set_run_parameters
import pandas as pd


def query(
    module: str = None,
    incremental: bool = False,
    day_offset: int = 7,
    years: list = None,
    pre_cleanup: bool = False,
    post_cleanup: bool = True,
    client_settings_path: str = None
) -> pd.DataFrame:
    """
    De query functie bevraagt de easyflex API op basis van de gekozen module en jaren. Het resultaat
    wordt weggeschreven naar de folder tmp/

    Parameters
    ----------
    module: str
        modulenaam van uit te vragen data
    incremental: bool
        er kan gekozen voor een incremental update, waarin de datumgewijzigd kolom wordt gefilterd.
    day_offset: int
        aantal dagen geleden aangepast. alleen nodig als incremental = True.
    years: list
        lijst aan jaren die uitgevraagd moeten worden.
    pre_cleanup: bool
        True als de mappen verwijderd moeten worden voordat het script wordt gedraaid.
    post_cleanup: bool
        True als de mappen verwijderd moeten worden nadat het script is gedraaid.
    client_settings_path: str
        Pad naar YAML met client_settings

    Returns
    -------
    df: pd.DataFrame
        dataframe met de resultaten van de query.

    """
    debug = False  # deze moet uiteindelijk uit de functies gehaald worden

    run_params = set_run_parameters(client_settings_path)

    if pre_cleanup:
        cleanup(run_params.pickledir, ext=".pkl")
    query_all(
        run_params,
        module=module,
        incremental=incremental,
        day_offset=day_offset,
        years=years,
        debug=debug,
    )
    if post_cleanup:
        cleanup(run_params.stagingdir, ext=".csv")

    df = cat_modules(run_params, module)

    return df
