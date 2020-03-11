from linkedin_api.linkedin import Linkedin
import json
import pandas as pd

config = json.load(open('config.json', encoding="utf8"))

companies = config["companies"]


class MutualConnections():
    def __init__(self, user, pwd, urn):
        self.api = Linkedin(user, pwd)
        self.urn = urn

    def get_connections(self, file_name=None, urn_id=None):
        if file_name:
            data = json.load(open(file_name, encoding="utf8"))
        else:
            data = self.api.get_profile_connections(urn_id)
        df = pd.DataFrame(data)
        df.columns = ["distance", "profile name",  "urn"]
        return df

    def get_my_urn_id(self):
        profile = self.api.get_profile(self.urn)
        return profile['entityUrn'].split(":")[-1]

    def get_company_urn(self, company_name=None):
        company = self.api.get_company(company_name)
        return company['url'].split('/')[-1]

    def get_first_two_connections(self, company_urn_id=None):
        return self.api.search_people(network_depth=["F", "S"], current_company=[company_urn_id], limit=2)

if __name__ == 'main':
    pass

conn_inst = MutualConnections(user=config["user"], pwd=config["pwd"], urn=config["urn"])

my_urn_id = conn_inst.get_my_urn_id()
my_connections_df = conn_inst.get_connections(file_name='my_connections.json', urn_id=my_urn_id)

output_df = pd.DataFrame()

for company in companies:
    _company_df = pd.DataFrame(columns=my_connections_df.columns)
    company_urn = conn_inst.get_company_urn(company_name=company)
    main_connections = conn_inst.get_first_two_connections(company_urn_id=company_urn)
    for connection in main_connections:
        sub_connection_df = conn_inst.get_connections(urn_id=connection['urn_id'])

        _company_df = pd.merge(my_connections_df, sub_connection_df, on='urn', how="inner", suffixes=('', '_y'))
        _company_df.drop(_company_df.filter(regex='_y').columns, axis=1, inplace=True)

        _company_df["Company"] = company
        _company_df["Primary connection"] = connection['public_id']

        output_df = output_df.append(_company_df, sort=False)

output_df.to_csv('output_df.csv')