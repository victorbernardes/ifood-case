SELECT
    HOUR(data_inicio_viagem) AS hora_do_dia,
    AVG(quantidade_passageiros) AS media_passageiros
FROM dados_taxi_silver.corridas_taxi

SELECT
    HOUR(data_inicio_viagem) AS hora_do_dia,
    AVG(quantidade_passageiros) AS media_passageiros
FROM dados_taxi_silver.corridas_taxi
WHERE mes = 5
GROUP BY HOUR(data_inicio_viagem)
ORDER BY hora_do_dia;
