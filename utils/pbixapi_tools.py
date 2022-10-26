import requests
import msal

class Refresh_pbix_web_api():
    def __init__(self):
        self.username = 'Digital_scm@us.q-cells.com'
        self.password = 'USscm123**'
        self.app_id = '54d02091-49e9-4bfa-8983-a8d6d61db2c5'
        self.tenant_id = '133df886-efe0-411c-a7af-73e5094bbe21'
        self.dataset_id = 'e5892459-615a-403f-8e8e-3134a329e044'

    def request_access_token(self):
        authority_url = 'https://login.microsoftonline.com/' + self.tenant_id
        scopes = ['https://analysis.windows.net/powerbi/api/.default']
        client = msal.PublicClientApplication(self.app_id, authority=authority_url)
        token_response = client.acquire_token_by_username_password(username=self.username, password=self.password, scopes=scopes)
        if not 'access_token' in token_response:
            raise Exception(token_response['error_description'])
        access_id = token_response.get('access_token')
        return access_id

    def request_refresh(self):
        try:
            access_id = self.request_access_token()
            endpoint = f'https://api.powerbi.com/v1.0/myorg/datasets/{self.dataset_id}/refreshes'
            headers = {'Authorization': f'Bearer ' + access_id}
            response = requests.get(endpoint, headers=headers)
            if response.status_code == 202:
                print('[EVENT] DATASET PBIX REFRESHED')
            else:
                print('[EVENT] DATASET PBIX REFRESHED')
        except:
            print('[WARNING] DATASET PBIX REFRESH ERROR')
            pass
    
if __name__ == '__main__':
    rbwa = Refresh_pbix_web_api()
    rbwa.request_refresh()
