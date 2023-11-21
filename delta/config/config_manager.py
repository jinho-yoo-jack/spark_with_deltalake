import yaml
import os
from pathlib import Path


class ConfigManager:
    def __init__(self):
        self.envMode = self.getEnvByKey()
        self.appConfig = None

    @classmethod
    def getEnvByKey(cls):
        env_name = os.getenv('PYTHON_EXECUTION_MODE')
        if env_name is None:
            env_name = 'dev'
        print(f'getEnvMode ::: {env_name}')
        return env_name

    def getConfigPath(self, mode):
        current_path = Path(os.path.dirname(os.path.realpath(__file__)))
        return f'{current_path}/../resources/application-{mode}.yml'

    def parserAppConfig(self):
        mode = self.envMode
        config_path = self.getConfigPath(mode)

        try:
            with open(config_path, 'r') as file:
                self.appConfig = yaml.safe_load(file)
        except FileNotFoundError:
            print(f'application-{mode}.yml is not found!!!')

    # getAppConfigBy(hadoop, dfs, url)
    def getAppConfigBy(self, key, *args):
        path = ''
        for arg in args:
            index = args.index(arg)
            if index == 0:
                path = self.appConfig[key][arg]
            else:
                path = path[arg]
        return path


if __name__ == '__main__':
    cm = ConfigManager()
    cm.parserAppConfig()
    print(cm.getAppConfigBy('hadoop', 'dfs', 'url'))
