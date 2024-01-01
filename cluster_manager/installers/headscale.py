import os
import pathlib
from typing import Final


class HeadScaleHandler:
    HEAD_SCALE_VERSION: Final[str] = '0.22.3'
    HEAD_SCALE_CONFIG_PATH: Final[pathlib.Path] = '/etc/headscale/config.yaml'
    HEAD_SCALE_URL: Final[str] = 'server_url: http://localhost:30000'
    HEAD_SCALE_ADDR: Final[str] = 'listen_addr: 0.0.0.0:30000'

    def __init__(self):
        pass

    @staticmethod
    def check_if_headsscale_is_installed() -> bool:
        return os.system('systemctl status headscale') == 0

    @staticmethod
    def install_headscale() -> bool:
        status = False
        status &= os.system(f'wget --output-document=headscale.deb \
  https://github.com/juanfont/headscale/releases/download/v{HeadScaleHandler.HEAD_SCALE_VERSION}/headscale_{HeadScaleHandler.HEAD_SCALE_VERSION}_linux_amd64.deb') == 0
        status &= os.system(f'dpkg --install headscale.deb') == 0
        status &= os.system(f'systemctl enable headscale') == 0
        if not status:
            return False
        head_scale_config = HeadScaleHandler.HEAD_SCALE_CONFIG_PATH.read_text()
        HeadScaleHandler.HEAD_SCALE_CONFIG_PATH.write_text(
            head_scale_config.replace('server_url: http://127.0.0.1:8080', HeadScaleHandler.HEAD_SCALE_URL).replace(
                'listen_addr: 127.0.0.1:8080', HeadScaleHandler.HEAD_SCALE_ADDR))
        status &= os.system(f'systemctl start headscale') == 0
        return status and os.system(f'systemctl status headscale') == 0


if __name__ == '__main__':
    HeadScaleHandler.install_headscale()
