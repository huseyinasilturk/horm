package horm

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"gorm.io/gorm"
	"gorm.io/gorm/schema"
)

func RemoveWithGetDeleted(db *gorm.DB, args interface{}, columns ...string) ([]map[string]interface{}, error) {
	//DESC - A variable is created to hold all deleted relationships
	var returnModel []map[string]interface{}

	//DESC - rg is created to get all the values ​​in the struct whose type is unknown in a controlled manner. The condition will be created with the field values ​​in this rg.
	rg := reflect.ValueOf(args)

	//DESC - To get where condition a all columns sent, column length is taken
	fieldCount := reflect.TypeOf(args).NumField()
	//DESC - To keep the column reached by loop
	columnValue := ""
	//DESC - We take the previous one of all the columns that come with the loop. Thus, we can put the (and) condition between the previous column and the first column.
	andContition := ""
	//DESC - to generate the data of the table to be deleted and the delete query
	mainWhereCondition := ""
	//DESC - To keep the entire query
	mainCondition := ""

	//DESC - To establish a query according to the filled columns of the sent struct
	for i := 0; i < fieldCount; i++ {
		//DESC - Clearing previous column
		columnValue = ""
		//DESC - Type checking of incoming columns
		switch reflect.ValueOf(args).Field(i).Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			columnValue = strconv.FormatUint(rg.Field(i).Uint(), 10)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			columnValue = strconv.FormatInt(rg.Field(i).Int(), 10)
		case reflect.Float32, reflect.Float64:
			columnValue = strconv.FormatFloat(rg.Field(i).Float(), 'f', -1, 64)
		case reflect.String:
			columnValue = "'" + rg.Field(i).String() + "'"
		default:
			//DESC - If the type is not one of the above, it is considered null to avoid processing
			columnValue = ""
		}
		//DESC - If the column is not empty, it is added to the query
		if len(strings.ReplaceAll(strings.TrimSpace(columnValue), "'", "")) != 0 {
			//DESC - If the first column is not empty and its types are appropriate, (and) is added in between.
			if i == fieldCount-1 || len(strings.ReplaceAll(strings.TrimSpace(andContition), "'", "")) != 0 {
				mainWhereCondition = mainWhereCondition + " AND "
			}
			mainWhereCondition += reflect.TypeOf(args).Field(i).Name + " =  " + columnValue + " "
		}
		//DESC - The column added to the condition is retained as the old column
		andContition = columnValue
	}

	//DESC - Define the db for the generated query
	tx := db.Begin()

	//DESC - in gorm as the incoming struct model to use defined
	model := tx.Model(args)

	//DESC - Get the database name of the struct you want to delete
	s, _ := schema.Parse(args, &sync.Map{}, schema.NamingStrategy{})
	mainTableName := s.Table

	//DESC - Query merged
	mainCondition = "WITH tabletemp (id) AS (SELECT * FROM " + mainTableName + " WHERE " + mainWhereCondition + "), " + "get" + mainTableName + " as ( Delete from " + mainTableName + " where id IN (select id from tabletemp)  RETURNING *), "

	//DESC - If you want to make the deletion request with a different column, not the ID, the query held to get the id of the deleted items.
	Query := ""
	//DESC - To keep the full join relationship to fetch all deleted columns
	joinQuery := ""
	for key, column := range columns {

		//DESC - Methods of relationships were created
		relation := model.Association(column)

		//DESC - If there is no relation return error
		if relation.Error != nil {
			return nil, errors.New("can't find any relation as ")
		}

		//DESC - If relation type is not m2m return error
		if relation.Relationship.Type != "many_to_many" {
			return nil, errors.New("can't find any m2m relation as " + column)
		}

		//DESC - Get model name to remove pivot data
		removeArgName := relation.Relationship.JoinTable.Name

		//DESC - Get first model relation foreign column name
		firstArgColumn := relation.Relationship.References[0].ForeignKey.DBName
		//DESC - Get last model relation foreign colum name

		//DESC - Create save point to rollback without deleting pivot data
		tx.SavePoint("notDeleteAbility")

		//DESC - Deletion for each model to be deleted.A virtual table with deleted ids after the deletion process
		Query = Query + " get_" + removeArgName + " As  (Delete from " + removeArgName + " where " + firstArgColumn + " IN (select id from tabletemp)  RETURNING *)"
		if key < len(columns)-1 {
			Query += ","
		}
		//DESC - To bring all of the deleted
		joinQuery = joinQuery + " full outer join get_" + removeArgName + " ON get_" + removeArgName + "." + firstArgColumn + " = tabletemp.id "

	}

	//DESC - Question is thrown to Database
	db.Raw(mainCondition + Query + " SELECT * FROM tabletemp " + joinQuery).Scan(returnModel)

	return returnModel, nil
}

func Sync(db *gorm.DB, args interface{}, column string, value []uint) (interface{}, error) {

	//DESC - If the incoming "arg" contains a single element, it must be enclosed in slice to be able to loop. A new slice was created for this
	var arg interface{}
	//DESC - Getting incoming "arg" type to be able to check
	argsType := reflect.TypeOf(args)

	//DESC - Creating a new slice of the incoming "arg" type
	argSlice := reflect.New(reflect.SliceOf(argsType)).Elem()

	if argsType.Kind() == reflect.Struct {
		//DESC - Since the element is singular, we enclose it in arg. It will be used later for type and relation checks inside
		arg = args
		//DESC - If the incoming "arg" is a single element, it is enclosed in a slice
		argSlice = reflect.Append(argSlice, reflect.ValueOf(args))
	} else if argsType.Kind() == reflect.Slice {
		//DESC - Since the element is plural, we take the first element of the slice and enclose it in arg. It will be used later for type and relation checks inside
		arg = reflect.ValueOf(args).Index(0).Interface()
		//DESC - If the incoming "arg" is a slice, it is assigned to argSlice unchanged.
		argSlice = reflect.ValueOf(args)
	} else {
		//DESC - If the incoming "arg" is not a slice or a single element, an error is returned
		return reflect.ValueOf(args), errors.New("args type error")
	}

	//DESC - Looping through the incoming "arg" elements
	argCount := argSlice.Len()

	//DESC - Creating a slice to store the IDs of the incoming "arg" elements
	argIDs := make([]uint, 0)

	for i := 0; i < argCount; i++ {
		//DESC - Getting the current element
		argIndex := argSlice.Index(i)
		if i < argCount-1 {
			//DESC - If the current item is not the last item, the if condition is entered for type checking
			if reflect.Value(argIndex).Type().Name() != reflect.Value(argSlice.Index(i+1)).Type().Name() {
				return nil, errors.New("all models submitted must be of the same type")
			}
		}
		//DESC - Set first model IDs value
		argIDs = append(argIDs, uint(argIndex.FieldByName("ID").Uint()))
	}

	tx := db.Begin()
	//DESC - Get relation name
	relation := tx.Model(arg).Association(column)

	//DESC - If there is no relation return error
	if relation.Error != nil {
		return nil, errors.New("can't find any relation as ")
	}

	//DESC - If relation type is not m2m return error
	if relation.Relationship.Type != "many_to_many" {
		return nil, errors.New("can't find any m2m relation as " + column)
	}

	//DESC - Get model name to remove pivot data
	removeArgName := relation.Relationship.JoinTable.Name

	//DESC - Get first model relation foreign column name
	firstArgColumn := relation.Relationship.References[0].ForeignKey.DBName
	//DESC - Get last model relation foreign colum name
	lastArgColumn := relation.Relationship.References[1].ForeignKey.DBName

	//DESC - Create save point to rollback without deleting pivot data
	tx.SavePoint("notDeleteAbility")

	//DESC - Delete pivot data
	tx.Table(removeArgName).Where(firstArgColumn, argIDs).Delete(argIDs)
	//DESC - Store data to create
	var dynamicTemp []map[string]interface{}
	var argID uint
	var temp uint
	//DESC - Looping through the incoming "arg" elements
	for _, argID = range argIDs {
		//DESC - Looping through the incoming "arg" elements
		for _, temp = range value {
			dynamicTemp = append(dynamicTemp, map[string]interface{}{
				firstArgColumn: argID,
				lastArgColumn:  temp,
			})
		}
	}

	//DESC - Insert created data to postqre
	crtStmt := tx.Table(removeArgName).Create(&dynamicTemp)

	//DESC - If can't insert, rollback and return error
	if crtStmt.Error != nil {
		tx.RollbackTo("notDeleteAbility")
		return nil, errors.New("Can't success to create any record on join table at " + removeArgName + " and rollbacked the query.")
	}
	tx.Commit()
	//DESC - Return created data
	return dynamicTemp, nil
}
