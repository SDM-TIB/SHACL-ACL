PREFIX wb: <http://worldbank.org/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX time: <http://www.w3.org/2006/time#>
PREFIX dc: <http://purl.org/dc/elements/1.1/>

SELECT DISTINCT ?year ?life_exp WHERE {
    ?country a wb:Country .
    ?country dc:identifier "DEU" .
    ?country wb:hasAnnualIndicatorEntry ?annualIndicator .
    ?annualIndicator wb:hasIndicator <http://worldbank.org/Indicator/SP.DYN.LE00.IN> .
    ?annualIndicator owl:hasValue ?life_exp .
    ?annualIndicator time:year ?year .
}
ORDER BY ?year
LIMIT 3
