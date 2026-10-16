section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "dim_date_2021_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared dim_date_2021 = let
    DayNamesPl = {"poniedziałek", "wtorek", "środa", "czwartek", "piątek", "sobota", "niedziela"},
    DayNamesDe = {"Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"},
    Source = Lakehouse.Contents(),
    Workspace = Source{[workspaceId = "50be2564-b12a-419d-93e8-fce57529a5b2"]}[Data],
    LakehouseItem = Workspace{[lakehouseId = "d92fa7a4-9836-40ba-918c-9446d5278216"]}[Data],
    DimDate = LakehouseItem{[Id = "dim_date", ItemKind = "Table"]}[Data],
    Filtered = Table.SelectRows(DimDate, each [year] = 2021),
    WithPl = Table.AddColumn(Filtered, "day_name_pl", each DayNamesPl{Date.DayOfWeek([date], Day.Monday)}, type text),
    WithDe = Table.AddColumn(WithPl, "day_name_de", each DayNamesDe{Date.DayOfWeek([date], Day.Monday)}, type text)
in
    WithDe;
shared dim_date_2021_DataDestination = let
    Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
    Navigation_1 = Pattern{[workspaceId = "50be2564-b12a-419d-93e8-fce57529a5b2"]}[Data],
    Navigation_2 = Navigation_1{[lakehouseId = "d92fa7a4-9836-40ba-918c-9446d5278216"]}[Data],
    TableNavigation = Navigation_2{[Id = "dim_date_2021", ItemKind = "Table"]}?[Data]?
in
    TableNavigation;
