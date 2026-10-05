// =====================================================================================
// Consultas Power Query (M) para o MTG_2 ler a camada gold do lake.
// Cada bloco abaixo é UMA consulta: no Power BI Desktop, Página Inicial > Transformar dados >
// Nova fonte > Consulta nula > Editor Avançado, cole o bloco e dê o nome indicado.
//
// Não precisa de gateway: os arquivos são lidos por HTTPS (fonte "Web", autenticação Anônima).
// =====================================================================================


// ---- Parâmetro: LakeUrl -------------------------------------------------------------
// Gerenciar Parâmetros > Novo parâmetro > Nome: LakeUrl, Tipo: Texto,
// Valor: URL pública do bucket, SEM barra no final. Ex.: https://pub-0123456789abcdef.r2.dev


// ---- Parâmetro: MesesHistorico ------------------------------------------------------
// Novo parâmetro > Nome: MesesHistorico, Tipo: Número decimal, Valor: 0
// 0 = carrega todo o histórico de preços; 6 = só os últimos 6 meses (modelo mais leve).


// ---- Consulta: Manifesto (desabilite "Habilitar carga" nela) ------------------------
let
    // O manifest.json lista todos os arquivos publicados. RelativePath mantém a URL base fixa,
    // o que permite a atualização agendada no Power BI Service.
    Fonte = Json.Document(
        Web.Contents(LakeUrl, [RelativePath = "gold/manifest.json", Headers = [#"Cache-Control" = "no-cache"]])
    )
in
    Fonte


// ---- Consulta: fnLerParquet (função; desabilite "Habilitar carga") ------------------
(caminho as text) as table =>
let
    // Query "v" muda a cada publicação e evita receber uma versão em cache do arquivo
    Binario = Web.Contents(LakeUrl, [RelativePath = caminho, Query = [v = Manifesto[publicado_em]]]),
    Tabela = Parquet.Document(Binary.Buffer(Binario))
in
    Tabela


// ---- Consulta: dim_cards ------------------------------------------------------------
// Repita este modelo para: dim_prints, dim_sets, dim_keywords, bridge_card_keywords,
// bridge_print_keywords, keyword_pairs, agg_set_verbose, agg_year_verbose
// (troque apenas o nome em Manifesto[tabelas][...]).
let
    Fonte = fnLerParquet(Manifesto[tabelas][dim_cards])
in
    Fonte


// ---- Consulta: fct_card_prices ------------------------------------------------------
let
    Arquivos = Manifesto[fatos][fct_card_prices],          // um Parquet por mês, em ordem
    Selecionados = if MesesHistorico > 0 then List.LastN(Arquivos, MesesHistorico) else Arquivos,
    Tabelas = List.Transform(Selecionados, each fnLerParquet(_)),
    Combinado = Table.Combine(Tabelas),
    Tipos = Table.TransformColumnTypes(
        Combinado,
        {
            {"card_id", type text}, {"price_date", type date},
            {"usd", Currency.Type}, {"usd_foil", Currency.Type}, {"usd_etched", Currency.Type},
            {"eur", Currency.Type}, {"eur_foil", Currency.Type}, {"tix", Currency.Type},
            {"loaded_at", type datetime}
        }
    )
in
    Tipos


// ---- Consulta: fct_print_prices -----------------------------------------------------
let
    Arquivos = Manifesto[fatos][fct_print_prices],
    Selecionados = if MesesHistorico > 0 then List.LastN(Arquivos, MesesHistorico) else Arquivos,
    Tabelas = List.Transform(Selecionados, each fnLerParquet(_)),
    Combinado = Table.Combine(Tabelas),
    Tipos = Table.TransformColumnTypes(
        Combinado,
        {
            {"print_id", type text}, {"price_date", type date},
            {"usd", Currency.Type}, {"usd_foil", Currency.Type}, {"usd_etched", Currency.Type},
            {"eur", Currency.Type}, {"eur_foil", Currency.Type}, {"tix", Currency.Type},
            {"loaded_at", type datetime}
        }
    )
in
    Tipos


// ---- Consulta: Atualizacao (opcional: cartão "dados de" no relatório) ---------------
let
    Fonte = #table(
        type table [snapshot_date = date, publicado_em = datetimezone],
        {{Date.FromText(Manifesto[snapshot_date]), DateTimeZone.FromText(Manifesto[publicado_em])}}
    )
in
    Fonte
