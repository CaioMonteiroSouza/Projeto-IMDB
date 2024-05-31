SELECT
	SUBSTRING(CAST(UTCNOW() AS STRING), 0, 11),
	COUNT(*) AS VOOS_COM_FATALIDADES,  
	SUM((CASE 
		WHEN s.Lesoes_Fatais_Passageiros IS NULL OR s.Lesoes_Fatais_Passageiros = 'null' OR s.Lesoes_Fatais_Passageiros = '0' THEN 0 
		ELSE 1
		END)) AS Soma_Lesoes_Fatais_Passageiros
FROM s3object s
WHERE 
	(s.Lesoes_Fatais_Tripulantes <> '' AND (s.Lesoes_Fatais_Tripulantes <> '0' AND s.Lesoes_Fatais_Tripulantes <> 'null')) 
	OR (s.Lesoes_Fatais_Passageiros <> '' AND (s.Lesoes_Fatais_Passageiros <> '0' AND s.Lesoes_Fatais_Passageiros <> 'null')) 
	OR (s.Lesoes_Fatais_Terceiros <> '' AND (s.Lesoes_Fatais_Terceiros <> '0' AND s.Lesoes_Fatais_Terceiros <> 'null'))