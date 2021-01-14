import logging
import os
import xml.etree.ElementTree as ET

import pandas as pd
from tqdm import tqdm

from easyflex.dataservices.soap_request import OperatieParameters
from easyflex.dataservices.soap_request import (
    get_data,
    namespaces,
    send_soap_request,
    check_for_errors,
    parse_records_v2,
)
from easyflex.functions import cat_modules
from .fields import pull_fields
from .format import format_data


def parse_lsg_array(element, regnr, loonspecnummer):
    ns, ns_txt = namespaces()
    records = list()

    loonkosten = element.findall("urn:item", ns)
    logging.debug(f"aantal loonkosten types in loonspecificatie: {len(loonkosten)}")

    if not len(loonkosten):
        return {}

    for kosten in loonkosten:
        d = parse_records_v2(kosten)
        d["registratienummer"] = regnr
        d["loonspecificatienummer"] = loonspecnummer
        records.append(d)

    return records


def parse_loonspecificatie_loonkosten(loonspec):
    ns, ns_txt = namespaces()
    all_records = list()
    regnr = loonspec.find("urn:ds_fw_loonspecificatiegegevens_registratienummer", ns).text
    loonspecnummer = loonspec.find(
        "urn:ds_fw_loonspecificatiegegevens_loonspecificatienummer", ns
    ).text

    for element in loonspec:
        fieldname = element.tag.replace(ns_txt["urn"], "")
        if fieldname == "ds_fw_loonspecificatiegegevens_loonkosten":
            records = parse_lsg_array(element, regnr, loonspecnummer)
        else:
            continue

        all_records = all_records + records

    return all_records


def parse_loonkosten(content, operatie):
    ns, ns_txt = namespaces()
    loonspecs = content.findall("urn:{}_result/urn:fields/urn:item".format(operatie.naam), ns)
    logging.debug(f"aantal loonspecificaties: {len(loonspecs)}")

    records = list()
    for loonspec in loonspecs:
        records = records + parse_loonspecificatie_loonkosten(loonspec)

    df = pd.DataFrame(records)
    df.columns = [col.replace(operatie.naam[3:] + "_", "") for col in df.columns]
    df.columns = [operatie.output_prefix + col for col in df.columns]  # add prefix

    return df


def get_loonkosten(run_params, operatie):
    ns, ns_txt = namespaces()

    response = send_soap_request(run_params, operatie)
    content = ET.fromstring(response.content.decode("utf-8")).find("ds:Body", ns)

    check_for_errors(content)

    df = parse_loonkosten(content, operatie)

    logging.debug(
        "{} - {} - {} records pulled (runcount: {})".format(
            run_params.adm_code, operatie.naam, len(df), operatie.runcount
        )
    )
    return df


def pull_module(run_params, module_name):
    logging.info("let op: dit script gebruikt data uit de module ds_fw_persoonsgegevens_all.")
    data = pd.DataFrame()
    data_loonkosten = pd.DataFrame()
    velden = pull_fields()
    regnrs = cat_modules(
        run_params, "{}_ds_fw_persoonsgegevens_all.pkl".format(run_params.adm_code)
    )
    regnrs = regnrs.fw_registratienummer.drop_duplicates().to_list()
    if run_params.debug:
        regnrs = regnrs[:10]

    logging.info("loonspecificatie gegevens opvragen voor {} flexwerkers".format(len(regnrs)))
    for regnr in tqdm(regnrs, desc="loonspecificaties downloaden van flexwerkers"):
        runcount = 0
        run = True

        while run:
            parameters = {"registratienummer": regnr}
            operatie = OperatieParameters(
                naam="ds_fw_loonspecificatiegegevens",
                output_prefix="lsg_",
                parameters=parameters,
                fields=velden,
                limit=300,
                runcount=runcount,
                incremental=run_params.incremental,
                day_offset=run_params.day_offset,
            )

            df = get_data(run_params, operatie)
            data = pd.concat([data, df], sort=False, ignore_index=True)

            df_loonkosten = get_loonkosten(run_params, operatie)
            data_loonkosten = pd.concat(
                [data_loonkosten, df_loonkosten], sort=False, ignore_index=True
            )

            if not len(df) == operatie.limit or run_params.debug:
                run = False

            runcount += 1

    if len(data):
        export_module(data, module_name, run_params)

    if len(data_loonkosten):
        export_module(
            data=data_loonkosten, module_name="ds_fw_loonspecificatiekosten", run_params=run_params
        )


def export_module(data, module_name, run_params):
    data = format_data(data)
    data["werkmaatschappij"] = run_params.adm_code
    data["lsg_id"] = data["werkmaatschappij"] + " - " + data["lsg_loonspecificatienummer"]
    filename = "{}_{}.pkl".format(run_params.adm_code, module_name)
    data.to_pickle(os.path.join(run_params.pickledir, filename))
    logging.info("{} geexporteerd! ({} records)".format(filename, len(data)))


def scoping():
    return [
        "3216279",
        "3231158",
        "3619377",
        "4066699",
        "4079055",
        "4107302",
        "4147617",
        "4155454",
        "4162184",
        "4200569",
        "4202938",
        "4220851",
        "4309083",
        "4309126",
        "4314235",
        "4338609",
        "4339183",
        "4344984",
        "4351805",
        "4354713",
        "4358001",
        "4372770",
        "4382661",
        "4387177",
        "4388605",
        "4388797",
        "4394489",
        "4399330",
        "4404442",
        "4438898",
        "4438920",
        "4448812",
        "4460919",
        "4488977",
        "4489014",
        "4489111",
        "4490043",
        "4493850",
        "4508563",
        "4509464",
        "4511368",
        "4512181",
        "4512236",
        "4540312",
        "4542439",
        "4552294",
        "4553696",
        "4557567",
        "4558395",
        "4558405",
        "4572412",
        "4577738",
        "4578719",
        "4586255",
        "4586283",
        "4586317",
        "4586330",
        "4586357",
        "4586492",
        "4588144",
        "4588398",
        "4594692",
        "4603779",
        "4613120",
        "4618809",
        "4619005",
        "4620189",
        "4622805",
        "4622815",
        "4622835",
        "4623705",
        "4623713",
        "4624676",
        "4629876",
        "4632735",
        "4632742",
        "4632744",
        "4645289",
        "4648595",
        "4649133",
        "4649674",
        "4650092",
        "4650842",
        "4652279",
        "4664277",
        "4669939",
        "4679881",
        "4684603",
        "4684609",
        "4684629",
        "4684879",
        "4684935",
        "4685753",
        "4685772",
        "4685799",
        "4685838",
        "4685905",
        "4685998",
        "4688232",
        "4695614",
        "4696922",
        "4697904",
        "4701364",
        "4701397",
        "4703554",
        "4707081",
        "4715138",
        "4737009",
        "4744341",
        "4744363",
        "4744385",
        "4744647",
        "4753433",
        "4809406",
        "4833569",
        "4880230",
        "4881091",
        "4882374",
        "4884615",
        "4885296",
        "4885356",
        "4898161",
        "4898217",
        "4898330",
        "4907420",
        "4912308",
        "4914314",
        "4929671",
        "4934949",
        "4971999",
        "5043182",
        "5061259",
        "5061631",
        "5061717",
        "5063070",
        "5071038",
        "5077894",
        "5085762",
        "5102267",
        "5103008",
        "5104471",
        "5104499",
        "5104510",
        "5104800",
        "5108245",
        "5108260",
        "5108278",
        "5110702",
        "5113352",
        "5113598",
        "5113800",
        "5116962",
        "5119013",
        "5119227",
        "5119806",
        "5124376",
        "5124608",
        "5124824",
        "5128948",
        "5130454",
        "5136662",
        "5136780",
        "5140335",
        "5140449",
        "5152440",
        "5157899",
        "5167410",
        "5169829",
        "5175765",
        "5178431",
        "5179273",
        "5207673",
        "5207698",
        "5215489",
        "5221360",
        "5243094",
        "5243709",
        "5249869",
        "5257203",
        "5260426",
        "5270379",
        "5283056",
        "5307147",
        "5313397",
        "5319470",
        "5319471",
        "5324938",
        "5333336",
        "5341258",
        "5353669",
        "5368602",
        "4080130",
        "4172541",
        "4235906",
        "4275422",
        "4313236",
        "4340822",
        "4344883",
        "4368411",
        "4382618",
        "4382700",
        "4448751",
        "4493954",
        "4557464",
        "4573222",
        "4603791",
        "4604963",
        "4606336",
        "4612764",
        "4620171",
        "4641224",
        "4650804",
        "4664279",
        "4695672",
        "4703561",
        "4843459",
        "4880262",
        "4880368",
        "4882792",
        "4891408",
        "4910703",
        "4910786",
        "4935614",
        "4937198",
        "4949991",
        "4961010",
        "4982236",
        "5019469",
        "5019494",
        "5032959",
        "5043188",
        "5050517",
        "5061138",
        "5061659",
        "5063091",
        "5096079",
        "5096317",
        "5097966",
        "5102279",
        "5103655",
        "5104579",
        "5104580",
        "5104720",
        "5105412",
        "5108204",
        "5108231",
        "5110719",
        "5113739",
        "5113886",
        "5116931",
        "5116977",
        "5118868",
        "5118938",
        "5118988",
        "5126202",
        "5132942",
        "5140225",
        "5141950",
        "5141997",
        "5153124",
        "5158241",
        "5170413",
        "5178577",
        "5179657",
        "5179742",
        "5187395",
        "5194200",
        "5214703",
        "5222169",
        "5226014",
        "5238589",
        "5243861",
        "5259782",
        "5270513",
        "5271300",
        "5277559",
        "5285590",
        "5296600",
        "5300143",
        "5311919",
        "5328085",
        "5328144",
        "5333272",
        "5382480",
        "4012648",
        "4202161",
        "4246915",
        "4401123",
        "4535227",
        "4628786",
        "4881146",
        "4900436",
        "4910854",
        "4911051",
        "4976954",
        "5043183",
        "5045662",
        "5050414",
        "5071533",
        "5077959",
        "5078414",
        "5095664",
        "5095714",
        "5096217",
        "5096223",
        "5096250",
        "5102784",
        "5102971",
        "5104333",
        "5104502",
        "5104548",
        "5105104",
        "5105302",
        "5105375",
        "5110730",
        "5113776",
        "5118899",
        "5118920",
        "5118962",
        "5119006",
        "5130762",
        "5132918",
        "5132968",
        "5136885",
        "5140320",
        "5147004",
        "5147070",
        "5158524",
        "5158547",
        "5158949",
        "5159174",
        "5163181",
        "5240677",
        "5339840",
    ]
