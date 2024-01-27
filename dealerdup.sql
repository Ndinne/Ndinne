--select * into snapfactauto_25_01_Copy from snapfactauto_25_01_View 


--DECLARE @BatchSize INT = 10;
--	DECLARE @RowCount INT = 1;
--	WHILE @RowCount > 0
--	BEGIN

		UPDATE Snapfactauto_25_01
		SET Suburb = SA.Suburb
		    Region = SA.Region,
		    Province = SA.Province
		FROM Snapfactauto_25_01 AS S
		INNER JOIN autocopyPROVINCE AS SA ON S.dealership = SA.Dealership
		WHERE SA.Dealership != 'Unavailable' AND S.Suburb IS NULL and S.[snap date] < '2023-07-23';

	--    SET @RowCount = @@ROWCOUNT;
	--END;

	--UPDATE Snapfactauto_25_01
	--SET Suburb = SA.Suburb, Region = SA.Region, Province = SA.Province
	--FROM autocopyPROVINCE AS SA
	--INNER JOIN autocopyPROVINCE AS DD ON SA.car_id = DD.car_id
	--WHERE SA.dealership = 'Unavailable';


	--UPDATE Snapfactauto_25_01
	--SET DEALERSHIP='AKSONS WHEELS A' 
	--WHERE CAR_ID IN(SELECT DISTINCT CAR_ID FROM Autocopyprovince WHERE DEALERSHIP='AKSONS WHEELS' AND [REGION]='Durban');

	--UPDATE Autocopyprovince SET DealershiP='AKSONS WHEELS A' WHERE DEALERSHIP='AKSONS WHEELS' AND [REGION]='Durban';
