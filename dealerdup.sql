
	UPDATE Snapfactauto_25_01
	SET Suburb = SA.Suburb, Region = SA.Region, Province = SA.Province
	FROM autocopyPROVINCE AS SA
	INNER JOIN autocopyPROVINCE AS DD ON SA.car_id = DD.car_id
	WHERE SA.dealership = 'Unavailable';


	UPDATE Snapfactauto_25_01
	SET DEALERSHIP='AKSONS WHEELS A' 
	WHERE CAR_ID IN(SELECT DISTINCT CAR_ID FROM Autocopyprovince WHERE DEALERSHIP='AKSONS WHEELS' AND [REGION]='Durban');

	UPDATE Autocopyprovince SET DealershiP='AKSONS WHEELS A' WHERE DEALERSHIP='AKSONS WHEELS' AND [REGION]='Durban';

