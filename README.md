# carbr-etl

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.24532759.svg)](https://doi.org/10.5281/zenodo.24532759)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Republica os imóveis rurais do **Cadastro Ambiental Rural (CAR)** — os dados públicos do Sicar, do
Serviço Florestal Brasileiro — como GeoParquet num bucket compatível com S3. É a origem dos dados
dos pacotes [carbr para R](https://github.com/StrategicProjects/carbr-r) e
[carbr para Python](https://github.com/StrategicProjects/carbr-py).

## O que faz

Para cada UF, um estado por vez:

1. **download** — lê a camada `sicar:sicar_imoveis_<uf>` do WFS público do Sicar em páginas de
   5 mil imóveis, sequenciais e com pausa, paginando por chave (`cod_imovel` maior que o último
   lido). Retoma de onde parou.
2. **build** — gera o GeoParquet do estado (ordenado por município e curva de Hilbert, grupos de
   4096 linhas, colunas `xmin`…`ymax` para filtro espacial), o resumo por município e o resumo
   pré-calculado por situação, porte, tipo, etapa da análise e ano de criação.
3. **publish** — envia os arquivos ao bucket e, por último, atualiza o `catalog.json`. Um estado
   cuja contagem caia mais de 5% em relação à publicada não é substituído.
4. **clean** — apaga os dados locais do estado.

```
current/
  catalog.json                          estados, datas, qualidade, SHA-256 dos arquivos
  dictionary.json                       dicionário de dados
  properties/state=<UF>/part-0.parquet  imóveis, com geometria (EPSG:4674)
  municipalities/state=<UF>.parquet     resumo por município
  summary/state=<UF>.parquet            resumo pré-calculado
```

## Uso

Precisa do [uv](https://docs.astral.sh/uv/); as dependências estão declaradas no próprio script.

```bash
cp .env.example .env && chmod 600 .env   # e preencha as credenciais

uv run etl/car_etl.py refresh        # atualização completa, todos os estados (cerca de 4 horas)
uv run etl/car_etl.py all            # só os estados que ainda não estão no catálogo
uv run etl/car_etl.py all AP DF      # refaz os estados indicados
uv run etl/car_etl.py verify         # confere o bucket contra o catálogo e o Sicar
uv run etl/car_etl.py summary        # regera o resumo pré-calculado a partir do bucket
```

Os passos também rodam separados: `download UF`, `build UF`, `publish UF`, `clean UF`.

## Agendamento

`deploy/` traz um serviço e um timer de usuário do systemd para a atualização semanal
(domingo, 1h):

```bash
mkdir -p ~/.config/systemd/user
cp deploy/car-etl.service deploy/car-etl.timer ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now car-etl.timer
loginctl enable-linger "$USER"            # para rodar sem sessão aberta

systemctl --user list-timers car-etl.timer
journalctl --user -u car-etl.service      # log da última rodada
```

O serviço espera o repositório em `~/carbr-etl`.

## Sobre os dados

- O Sicar não informa a data de alteração de um cadastro, só a de criação. Por isso cada
  atualização baixa tudo de novo.
- A publicação traz o perímetro declarado e dez atributos de cada imóvel. Não há dado pessoal.
- O cadastro é autodeclarado: há sobreposições, e a presença no CAR não comprova domínio.

## Como citar

Arquivado no Zenodo: [doi:10.5281/zenodo.24532759](https://doi.org/10.5281/zenodo.24532759) (o DOI aponta sempre para a versão
mais recente).

Código sob licença MIT.
