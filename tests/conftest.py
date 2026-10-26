import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from estudo.sessao import criar_sessao  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    sessao = criar_sessao("testes", master="local[2]", particoes=2)
    yield sessao
    sessao.stop()
