CREATE OR REPLACE VIEW desafio_final_t2.gold.vw_eventos_futuros AS
SELECT *
FROM desafio_final_t2.gold.eventos
WHERE data_evento > current_date()