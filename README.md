# FORELDET nvdb-arcmap-plugin

# Denne plugin'en bruker versjon 2 av NVDB api, med gamle administrative grenser og gammelt vegreferansesystem. 

Versjon 2 av NVDB api er **ikke** noe vi offisielt drifter [etter 1. august 2021](vegdata.no/info-utfasing-nvdb-klassisk/), men rent teknisk tar det kanskje litt tid før vi skrur det av. Plugin vi dermed rent tenkisk kunne fungere enda en stund. 

Vi har ingen planer om å oppgradere denne plugin'en til versjon 3 av NVDB api, fordi den ikke fungerer på arcgis pro. Men det er selvsagt fritt frem for andre å kopiere koden og jobbe videre slik de ønsker.

Vi har en rekke oppskrifter på alternative løsninger her: https://www.vegdata.no/ofte-stilte-sporsmal/hvordan-far-jeg-nvdb-data-inn-i-kartsystemet-mitt/ . For arcgis PRO brukere nevner vi spesielt at firmaet Geodata A/S har valgt å utvikle en kommersiell nedlastingstjeneste med NVDB data for sine kunder. 

--- 

Plugin for å hente NVDB-data inn til Esri desktop GIS verktøyene arcgis pro og arc map. Utviklet av Geodata A/S og Statens Vegvesen i fellesskap. 

https://www.vegdata.no/2017/07/07/rask-tilgang-til-nvdb-data-i-arcmap/ 

Versjon 2.06, med disse ad-hoc endringene / bugfiksene: 
 - Fiks av paginering (juli 2020)
 - Endret oppførsel for nedlasting av data med valget "bruk kartutsnitt". Nå brukes gjeldende kartutsnitt, ikke det første kartutsnittet som er lagret i ditt lokale prosjekt. 
 
 
