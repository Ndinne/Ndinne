DECLARE @StartDate DATE = '2023-07-18';
DECLARE @EndDate DATE = GETDATE(); -- Today's date
DECLARE @BatchSize INT = 100000; -- Adjust batch size as needed

DECLARE @CurrentDate DATE = @StartDate;

WHILE @CurrentDate <= @EndDate
BEGIN
    PRINT 'Updating records for ' + CONVERT(VARCHAR, @CurrentDate);

    UPDATE TOP (@BatchSize) Snapfactauto_25_01
    SET Snapfactauto_25_01.Dealer_ID = 
        CASE 
            WHEN Snapfactauto_25_01.Dealership = 'Unavailable' THEN '1807'
            ELSE ISNULL([Dim Dealer1].Dealer_ID, Snapfactauto_25_01.Dealer_ID) -- Update with [Dim Dealer1].Dealer_ID if not NULL
        END,
        Snapfactauto_25_01.VehicleID = 
        CASE 
            WHEN Snapfactauto_25_01.Dealership = 'Unavailable' THEN Autocopyprovince.vehid
            ELSE ISNULL(Autocopyprovince.vehid, Snapfactauto_25_01.VehicleID) -- Update with Autocopyprovince.vehid if not NULL
        END,
        Snapfactauto_25_01.RegionID = 
        CASE 
            WHEN Snapfactauto_25_01.Dealership = 'Unavailable' THEN Autocopyprovince.REGID
            ELSE ISNULL([Dim Dealer1].RegionID, Snapfactauto_25_01.RegionID) -- Update with [Dim Dealer1].RegionID if not NULL
        END
    FROM (
        SELECT *
        FROM Snapfactauto_25_01
        WHERE CONVERT(DATE, [snap date]) = @CurrentDate -- Filter records for current date
    ) AS Snapfactauto_25_01
    LEFT JOIN Autocopyprovince ON Snapfactauto_25_01.Car_ID = Autocopyprovince.Car_ID
    LEFT JOIN [Dim Dealer1] ON Snapfactauto_25_01.Dealership = [Dim Dealer1].DealershipName;
    --WHERE Snapfactauto_25_01.Dealer_ID IS NULL OR Snapfactauto_25_01.VehicleID IS NULL OR Snapfactauto_25_01.RegionID IS NULL;

    SET @CurrentDate = DATEADD(DAY, 1, @CurrentDate); -- Move to the next day
END





--select * into snapfactauto_25_01_Copy from snapfactauto_25_01_View 


--DECLARE @BatchSize INT = 10;
--	DECLARE @RowCount INT = 1;
--	WHILE @RowCount > 0
--	BEGIN

		--UPDATE Snapfactauto_25_01
		--SET Suburb = SA.Suburb,
		  --  Region = SA.Region,
		  --  Province = SA.Province
		--FROM Snapfactauto_25_01 AS S
		--INNER JOIN AutocopyprovincePROVINCE AS SA ON S.dealership = SA.Dealership
		--WHERE SA.Dealership != 'Unavailable' AND S.Suburb IS NULL and S.[snap date] < '2023-07-23';

	--    SET @RowCount = @@ROWCOUNT;
	--END;

	--UPDATE Snapfactauto_25_01
	--SET Suburb = SA.Suburb, Region = SA.Region, Province = SA.Province
	--FROM AutocopyprovincePROVINCE AS SA
	--INNER JOIN AutocopyprovincePROVINCE AS DD ON SA.car_id = DD.car_id
	--WHERE SA.dealership = 'Unavailable';


	--UPDATE Snapfactauto_25_01
	--SET DEALERSHIP='AKSONS WHEELS A' 
	--WHERE CAR_ID IN(SELECT DISTINCT CAR_ID FROM Autocopyprovinceprovince WHERE DEALERSHIP='AKSONS WHEELS' AND [REGION]='Durban');

	--UPDATE Autocopyprovinceprovince SET DealershiP='AKSONS WHEELS A' WHERE DEALERSHIP='AKSONS WHEELS' AND [REGION]='Durban';
