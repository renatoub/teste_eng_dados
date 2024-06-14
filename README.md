# Teste - Engenharia de Dados (Short Track)

Teste técnico para posições em Engenharia de Dados (short track)

O seguinte teste tem por premissa ser um problema base (Ref. <a href="hsrc/ttps://teaching.cornell.edu/teaching-resources/engaging-students/problem-based-learning">problem based learning</a>) de modo que você pode usá-lo como achar adequado tendo em vista a demonstração de seus conhecimentos técnicos.

Queremos entender melhor seu jeito de atacar problemas desafiadores em amostras de mundo real, além de que, do modo que está estruturada, a entrega, apesar de rápida, nos permite verificar mais detalhes de seu perfil profissional tais como organização, pontualidade e percepção quanto a suas skills de data wrangling, validação e governança, programação Python e entendimento dos serviços GCP (em especial Clod Run, BigQuery e BigTable).

Recomendamos ainda que o teste seja feito em no máximo 6 horas (não se preocupe em respostas muito detalhadas ou em complexidades que simplesmente não funcionariam no mundo real!).

As entregas devem ser via envio de link para um github público (ou similar) o qual contenha sua solução para o cenário a seguir:

## Cenário

No seguinte cenário você é a pessoa engenheira de dados por trás do projeto de data ops junto a uma grande operadora de cartões de crédito.

Os dados a serem ingeridos e analisados em nossa plataforma de Big Data são dados de compras (trasacao), documentos (contrato) e dados de compradores (cliente).

## Entregáveis

1) Sua primeira tarefa consiste em escrever uma aplicação para calcular o ganho total da empresa, o qual é obtido a partir da taxa administrativa do serviço de cartão de crédito para seus clientes. Esse ganho é calculado sobre um percentual das transações de cartão de crédito realizadas por eles. O cálculo é baseado no conjunto de dados abaixo, transacao, contrato e cliente da <a href="hsrc/ttps://drive.google.com/file/d/1lA2eLHNMoMpApPGz6h7WQpphT9URWxB1/view?usp=sharing">Figura 1</a>.

![Figura 1](assets/Figura%201.png)

O resultado esperado é uma consulta que retorne o ganho total da empresa por cliente que é 1.198,77 para o cliente A e 1,08 para o cliente B, conforme a <a href="hsrc/ttps://drive.google.com/file/d/1KJ9SvkcRX94YQDyKI01ivG-5N3lZp3T1/view?usp=sharing">Figura 2</a>.

![Figura 2](assets/Figura%202.png)

Assim sendo, seguem <a href="hsrc/ttps://drive.google.com/file/d/1lqZZb9WgkyyL7qBZ5ZAPENVYoioK2hMs/view?usp=sharing">snippet de código</a> para criação da base de dados e dos dados exemplos (via SQL Server).

<a href ="src/entregavel_1/README.md">Acessar resposta 1</a>

2) A segunda tarefa consiste em calcular o total líquido da empresa. Esse total é calculado da seguinte forma total_liquido = soma(total_bruto – desconto_percentual). O cálculo é baseado no conjunto de dados da <a href="hsrc/ttps://drive.google.com/file/d/1vekbII5FYAB57mMTwU9I64XRCATD_XqF/view?usp=sharing">Figura 3</a>

![Figura 3](assets/Figura%203.png)

O resultado esperado é uma código com pyspark que retorne o total liquido da empresa que é 59973.46. 

<a href ="src/entregavel_2/README.md">Acessar resposta 2</a>

3) O terceiro entregável consiste na transformação de dados disponíveis em <a href="hsrc/ttps://drive.google.com/file/d/1IDCjpDZh5St97jw4K_bAewJ8hf-rax9C/view?usp=sharing">arquivo Json</a> para o formato de dataframe, algo comum no dia a dia da empresa. Após transformar esse Json em dataframe é possível perceber que a coluna "item_list" está como dicionário. Seu gestor pediu dois pontos de atenção nessa tarefa:

- Expandir a coluna num mesmo dataframe;
- Normalizar os itens dessa coluna de dicionário e dividí-los em dois dataframes separados, seguindo o modelo relacional.

<a href ="src/entregavel_3/README.md">Acessar resposta 3</a>

~~~JSON
[
   {
      "CreateDate":"2021-05-24T20:21:34.79",
      "EmissionDate":"2021-05-24T00:00:00",
      "Discount":0.0,
      "NFeNumber":501,
      "NFeID":1,
      "ItemList":[
         {
            "ProductName":"Rice",
            "Value":35.55,
            "Quantity":2
         },
         {
            "ProductName":"Flour",
            "Value":11.55,
            "Quantity":5
         },
         {
            "ProductName":"Bean",
            "Value":27.15,
            "Quantity":7
         }
      ]
   },
   {
      "CreateDate":"2021-05-24T20:21:34.79",
      "EmissionDate":"2021-05-24T00:00:00",
      "Discount":0.0,
      "NFeNumber":502,
      "NFeID":2,
      "ItemList":[
         {
            "ProductName":"Tomate",
            "Value":12.25,
            "Quantity":10
         },
         {
            "ProductName":"Pasta",
            "Value":7.55,
            "Quantity":5
         }
      ]
   },
   {
      "CreateDate":"2021-05-24T20:21:34.79",
      "EmissionDate":"2021-05-24T00:00:00",
      "Discount":0.0,
      "NFeNumber":503,
      "NFeID":3,
      "ItemList":[
         {
            "ProductName":"Beer",
            "Value":9.00,
            "Quantity":6
         },
         {
            "ProductName":"French fries",
            "Value":10.99,
            "Quantity":2
         },
         {
            "ProductName":"Ice cream",
            "Value":27.15,
            "Quantity":1
         }
      ]
   }
]
~~~

4) Imagine que o Json das notas fiscais é disponibilizado em uma API. Como você utilizaria as tecnologias da GCP para ingerir, transformar e, eventualmente, carregar esses dados em um BigTable? O quarto entregável consiste na construção de uma arquitetura de ingestão dos dados de nota fiscal do entregável anterior (como visto <a href="hsrc/ttps://www.crystalloids.com/hs-fs/hubfs/Screenshot%202022-02-04%20at%2009-44-40-png.png?width=1232&name=Screenshot%202022-02-04%20at%2009-44-40-png.png">aqui</a>), a qual deve atender aos seguintes pontos:

![Figura 4](assets/Screenshot%202022-02-04%20at%2009-44-40-png.webp)

- Esquemas de fluxo de dados;
- Descrições de funcionamento (se necessário);
- Nomes de tecnologias em ecossistema GCP (serviços, conectores, bibliotecas e módulos).

Será apreciado como esforço extra se você conseguir avançar mais na aplicação além desse ponto.

Lembre-se que, como parte dos entregáveis anteriores, esperamos que alguns comentários sejam incluídos em suas soluções prévias; queremos entender melhor como foi seu processo de solução de problemas, quais as hipóteses levantadas e, se tivesse mais tempo, como você poderia melhorar a implementação proposta (desenvolvimento incremental).

Ou seja, temos quatro entregáveis:

- Consulta que retorne o ganho total da empresa por cliente;
- Código com pyspark que retorne o total liquido;
- Resolução de problema de transformação de dados (NF-e);
- Arquitetura exemplo da ingestão anterior (ecossistema GCP);

<a href ="src/entregavel_4/README.md">Acessar resposta 4</a>
