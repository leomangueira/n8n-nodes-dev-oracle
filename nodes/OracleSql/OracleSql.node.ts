import { IExecuteFunctions } from 'n8n-core';

import {
	ICredentialDataDecryptedObject,
	ICredentialsDecrypted,
	ICredentialTestFunctions,
	IDataObject,
	INodeCredentialTestResult,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';

import { Knex, knex } from 'knex';

import * as oracledb from 'oracledb';

import { ITables } from './TableInterface';

import {
	copyInputItem,
	createTableStruct,
	executeQueryQueue,
	extractDeleteValues,
	extractUpdateCondition,
	extractUpdateSet,
	extractValues,
	formatColumns,
} from './GenericFunctions';

export class OracleSql implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Oracle Database',
		name: 'oracleSql',
		icon: 'file:mssql.svg',
		group: ['input'],
		version: 1,
		description: 'Get, add and update data in Oracle Database',
		defaults: {
			name: 'Oracle Database',
		},
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'oracleSqlApi',
				required: true,
				testedBy: 'oracleSqlConnectionTest',
			},
		],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Execute Query',
						value: 'executeQuery',
						description: 'Execute an SQL query',
						action: 'Execute a SQL query',
					},
					{
						name: 'Insert',
						value: 'insert',
						description: 'Insert rows in database',
						action: 'Insert rows in database',
					},
					{
						name: 'Update',
						value: 'update',
						description: 'Update rows in database',
						action: 'Update rows in database',
					},
					{
						name: 'Delete',
						value: 'delete',
						description: 'Delete rows in database',
						action: 'Delete rows in database',
					},
				],
				default: 'insert',
			},

			// ----------------------------------
			//         executeQuery
			// ----------------------------------
			{
				displayName: 'Query',
				name: 'query',
				type: 'string',
				typeOptions: {
					alwaysOpenEditWindow: true,
				},
				displayOptions: {
					show: {
						operation: ['executeQuery'],
					},
				},
				default: '',
				placeholder: 'SELECT * FROM dual WHERE rownum = 1',
				required: true,
				description: 'The SQL query to execute',
			},

			// ----------------------------------
			//         insert
			// ----------------------------------
			{
				displayName: 'Table',
				name: 'table',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['insert'],
					},
				},
				default: '',
				required: true,
				description: 'Name of the table in which to insert data to',
			},
			{
				displayName: 'Columns',
				name: 'columns',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['insert'],
					},
				},
				default: '',
				// eslint-disable-next-line n8n-nodes-base/node-param-placeholder-miscased-id
				placeholder: 'id,name,description',
				description:
					'Comma-separated list of the properties which should used as columns for the new rows',
			},

			// ----------------------------------
			//         update
			// ----------------------------------
			{
				displayName: 'Table',
				name: 'table',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['update'],
					},
				},
				default: '',
				required: true,
				description: 'Name of the table in which to update data in',
			},
			{
				displayName: 'Update Key',
				name: 'updateKey',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['update'],
					},
				},
				default: 'id',
				required: true,
				// eslint-disable-next-line n8n-nodes-base/node-param-description-miscased-id
				description:
					'Name of the property which decides which rows in the database should be updated. Normally that would be "id".',
			},
			{
				displayName: 'Columns',
				name: 'columns',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['update'],
					},
				},
				default: '',
				placeholder: 'name,description',
				description:
					'Comma-separated list of the properties which should used as columns for rows to update',
			},

			// ----------------------------------
			//         delete
			// ----------------------------------
			{
				displayName: 'Table',
				name: 'table',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['delete'],
					},
				},
				default: '',
				required: true,
				description: 'Name of the table in which to delete data',
			},
			{
				displayName: 'Delete Key',
				name: 'deleteKey',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['delete'],
					},
				},
				default: 'id',
				required: true,
				// eslint-disable-next-line n8n-nodes-base/node-param-description-miscased-id
				description:
					'Name of the property which decides which rows in the database should be deleted. Normally that would be "id".',
			},
		],
	};

	methods = {
		credentialTest: {
			async oracleSqlConnectionTest(
				this: ICredentialTestFunctions,
				credential: ICredentialsDecrypted,
			): Promise<INodeCredentialTestResult> {
				const credentials = credential.data as ICredentialDataDecryptedObject;
				try {
					const config = {
						host: credentials.host as string,
						port: credentials.port as number,
						sid: credentials.sid as string,
						user: credentials.user as string,
						password: credentials.password as string,
						connectionTimeout: credentials.connectTimeout as number,
						requestTimeout: credentials.requestTimeout as number,
					};
					let pool = oracledb.createPool(config);
					(await pool).getConnection;
				} catch (error) {
					return {
						status: 'Error',
						message: error.message,
					};
				}
				return {
					status: 'OK',
					message: 'Connection successful!',
				};
			},
		},
	};

		async run(
			sourceOptions: SourceOptions,
			queryOptions: QueryOptions,
			dataSourceId: string,
			dataSourceUpdatedAt: string,
		): Promise<QueryResult> {
			let result = {
				rows: [],
			};
			let query = '';

			if (queryOptions.mode === 'gui') {
				if (queryOptions.operation === 'bulk_update_pkey') {
					query = await this.buildBulkUpdateQuery(queryOptions);
				}
			} else {
				query = queryOptions.query;
			}

			const knexInstance = await this.getConnection(
				sourceOptions,
				{},
				true,
				dataSourceId,
				dataSourceUpdatedAt,
			);

			// eslint-disable-next-line no-useless-catch
			try {
				result = await knexInstance.raw(query);

				return {
					status: 'ok',
					data: result,
				};
			} catch (err) {
				throw err;
			}
		}

		async testConnection(sourceOptions: SourceOptions): Promise<ConnectionTestResult> {
			const knexInstance = await this.getConnection(sourceOptions, {}, false);

			const result = await knexInstance.raw('SELECT * FROM v$version');

			return {
				status: 'ok',
			};
		}

		initOracleClient(clientPathType: string, customPath: string) {
			try {
				if (clientPathType === 'custom') {
					if (process.platform === 'darwin') {
						oracledb.initOracleClient({ libDir: process.env.HOME + customPath });
					} else if (process.platform === 'win32') {
						oracledb.initOracleClient({
							libDir: customPath,
						}); // note the double backslashes
					}
				} else {
					oracledb.initOracleClient();
				}
			} catch (err) {
				console.error(err);
				throw err;
			}
		}

		async buildConnection(sourceOptions: SourceOptions) {
			// we should add this to our datasource documentation
			try {
				oracledb.oracleClientVersion;
			} catch (err) {
				console.log('Oracle client is not initailized');
				this.initOracleClient(sourceOptions.client_path_type, sourceOptions.path);
			}

			const config: Knex.Config = {
				client: 'oracledb',
				connection: {
					user: sourceOptions.username,
					password: sourceOptions.password,
					connectString: `(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=${sourceOptions.host})(PORT=${sourceOptions.port}))(CONNECT_DATA=(SERVER=DEDICATED)(${sourceOptions.database_type}=${sourceOptions.database})))`,
					multipleStatements: true,
					ssl: sourceOptions.ssl_enabled, // Disabling by default for backward compatibility
				},
			};

			return knex(config);
		}

		async getConnection(
			sourceOptions: SourceOptions,
			options: any,
			checkCache: boolean,
			dataSourceId?: string,
			dataSourceUpdatedAt?: string,
		): Promise<any> {
			if (checkCache) {
				let connection = await getCachedConnection(dataSourceId, dataSourceUpdatedAt);

				if (connection) {
					return connection;
				} else {
					connection = await this.buildConnection(sourceOptions);
					dataSourceId && cacheConnection(dataSourceId, connection);
					return connection;
				}
			} else {
				return await this.buildConnection(sourceOptions);
			}
		}

		async buildBulkUpdateQuery(queryOptions: any): Promise<string> {
			let queryText = '';

			const tableName = queryOptions['table'];
			const primaryKey = queryOptions['primary_key_column'];
			const records = queryOptions['records'];

			for (const record of records) {
				queryText = `${queryText} UPDATE ${tableName} SET`;

				for (const key of Object.keys(record)) {
					if (key !== primaryKey) {
						queryText = ` ${queryText} ${key} = '${record[key]}',`;
					}
				}

				queryText = queryText.slice(0, -1);
				queryText = `begin ${queryText} WHERE ${primaryKey} = ${record[primaryKey]}; end;`;
			}

			return queryText.trim();
		}
	

}

