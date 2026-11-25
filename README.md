# carbr-etl

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.24532759.svg)](https://doi.org/10.5281/zenodo.24532759)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

Republica os imóveis rurais do **Cadastro Ambiental Rural (CAR)** — os dados públicos do Sicar, do
Serviço Florestal Brasileiro — como GeoParquet num bucket compatível com S3. É a origem dos dados
dos pacotes [carbr para R](https://github.com/castlaboratory/carbr-r) e
[carbr para Python](https://github.com/castlaboratory/carbr-py).

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

## Histórico

O Sicar não informa quando um cadastro muda de situação ou de etapa da análise. A cada atualização, antes
de substituir um estado, o ETL compara a carga publicada com a nova e guarda, fora de `current/`:

```
history/
  summary/date=<AAAA-MM-DD>/state=<UF>.parquet      o resumo daquela carga (mesmo formato de summary/)
  transitions/date=<AAAA-MM-DD>/state=<UF>.parquet  o fluxo desde a carga anterior
```

O arquivo de fluxo traz, por estado, quantos imóveis (e que área) passaram de cada situação e etapa para
cada outra: `from_date`, `to_date`, `from_status`, `to_status`, `from_condition`, `to_condition`,
`size_class`, `properties`, `area_ha`. `from_*` nulo é imóvel que entrou no cadastro; `to_*` nulo, que saiu.
As datas de cada estado ficam em `history` no `catalog.json`. A série começa em 25/11/2026.

Ao fim de cada rodada, o histórico e uma cópia datada do catálogo são copiados do bucket para
`data/history-backup/`, porque esses arquivos não podem ser refeitos a partir do Sicar.

## Indicadores de qualidade

Para cada imóvel não cancelado, `quality` calcula:

- `overlap_count` e `overlap_share`: com quantos outros imóveis do estado ele divide área, e que
  parcela da área dele está sobreposta (a soma das interseções com cada vizinho, limitada a 1);
- `outside_share`: que parcela da área fica fora do município declarado, pela malha municipal do IBGE;
- `indigenous_share`: que parcela fica dentro de terras indígenas, com o nome da principal em
  `indigenous_land`;
- `strict_protection_share` e `sustainable_use_share`: que parcela fica dentro de unidades de
  conservação de proteção integral e de uso sustentável, com o nome e a categoria da principal em
  `conservation_unit` e `conservation_category` (a de proteção integral, se houver).

```
current/
  quality/state=<UF>.parquet                 property_code, municipality_code e os indicadores
  quality-municipalities/state=<UF>.parquet  por município: imóveis com sobreposição acima de 1% e de
                                             50%, hectares sobrepostos, imóveis e hectares fora do município,
                                             em terra indígena e em unidade de conservação
```

As terras indígenas vêm do serviço de mapas da Funai (camada `tis_poligonais`, todas as fases do
processo). As unidades de conservação vêm do Cadastro Nacional de Unidades de Conservação (CNUC), pelo
mesmo pedido que o botão de download do [mapa do CNUC](https://cnuc.mma.gov.br/map) faz (dados
correntes); se ele falhar, do último shapefile semestral do portal de dados do MMA. Com
`CAR_PROTECTED_OFFLINE=1`, nada é baixado e o cálculo usa o que já está em disco. As duas camadas ficam em `data/protected/`
por 30 dias, e a edição usada vai para `source.protected_areas` no catálogo. Estar em unidade de uso
sustentável não é irregularidade: APA e RPPN admitem imóveis privados.

As parcelas são razões entre áreas no mesmo lugar, calculadas direto em graus. A sobreposição conta só
imóveis do mesmo estado. Na atualização semanal, o cálculo roda depois da publicação de cada estado; uma
falha nele fica no log e não impede a atualização dos dados.

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

## Página de consulta

`web/` é uma página estática, servida do mesmo endereço dos dados
(<https://car.castlab.org/index.html>): clicar num ponto do mapa, informar o código de um imóvel ou
escolher um município mostra o que está declarado, em linguagem simples. Ela lê os mesmos arquivos
Parquet direto do navegador, por requisições parciais, sem servidor de aplicação, e mostra quanto
cada consulta transferiu. As bibliotecas ficam em `web/vendor/` (ver o README de lá).

`web/ajuda.html` (<https://car.castlab.org/ajuda.html>) é a central de ajuda: o que é o CAR, o que significa
cada situação e cada etapa da análise, perguntas frequentes, como usar os dados, o dicionário de dados e a
legislação.

As duas páginas pedem cada arquivo com a assinatura do conteúdo no endereço (`?v=<início do SHA-256>`),
tirada do catálogo. Sem isso, depois de uma atualização o navegador mistura trechos que guardou do arquivo
antigo com os do novo e a leitura falha.

```bash
uv run etl/car_etl.py web            # envia web/ à raiz do bucket
```

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

### Aviso por e-mail

Ao fim de cada rodada, com sucesso ou falha, o serviço dispara `car-etl-notify@.service`, que envia um
resumo do log para o endereço em `NOTIFY_TO` (no `.env`). O envio usa o `msmtp`; `deploy/msmtprc.example`
traz a configuração para o relay SMTP do Google Workspace. Sem `NOTIFY_TO`, nada é enviado, e uma falha
no aviso não afeta a atualização.

```bash
sudo apt install msmtp
cp deploy/msmtprc.example ~/.msmtprc && chmod 600 ~/.msmtprc
cp deploy/car-etl-notify@.service ~/.config/systemd/user/ && systemctl --user daemon-reload
```

## Sobre os dados

- O Sicar não informa a data de alteração de um cadastro, só a de criação. Por isso cada
  atualização baixa tudo de novo.
- A publicação traz o perímetro declarado e dez atributos de cada imóvel. Não há dado pessoal.
- O cadastro é autodeclarado: há sobreposições, e a presença no CAR não comprova domínio.
- **Base normativa:** a Instrução Normativa MMA nº 3, de 18/12/2014, garante o livre acesso às
  informações não sigilosas e não pessoais do Sicar (arts. 12 e 18). São sigilosas as que
  identificam proprietários ou possuidores ou os associam aos imóveis (art. 4º); nenhuma delas
  está nesta publicação.
- **Finalidade:** pela mesma norma (art. 2º), as informações do CAR se destinam a subsidiar
  políticas, programas, projetos e atividades de controle, monitoramento, planejamento ambiental
  e econômico e combate ao desmatamento. O uso para outros fins que cause dano a terceiros pode
  gerar responsabilização.
- **Sem caráter oficial:** é uma republicação independente, feita pelo CastLab/UFPE. Não é um
  produto do Serviço Florestal Brasileiro nem substitui o Sicar; em caso de divergência, vale o
  Sicar.

## Como citar

Arquivado no Zenodo: [doi:10.5281/zenodo.24532759](https://doi.org/10.5281/zenodo.24532759) (o DOI aponta sempre para a versão
mais recente).

Código sob licença MIT.
