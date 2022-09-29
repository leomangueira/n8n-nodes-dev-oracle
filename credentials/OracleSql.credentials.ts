/* eslint-disable n8n-nodes-base/cred-filename-against-convention */
import { ICredentialType, INodeProperties } from 'n8n-workflow';

export class OracleSqlApi implements ICredentialType {
	name = 'oracleSqlApi';
	displayName = 'Oracle SQL API';
	documentationUrl = 'https://github.com/oracle/node-oracledb/';
	properties: INodeProperties[] = [
		{
			displayName: 'Host',
			name: 'host',
			type: 'string',
			default: '192.168.0.2',
		},
		{
			displayName: 'Port',
			name: 'port',
			type: 'number',
			default: 1521,
		},
		{
			displayName: 'SID',
			name: 'sid',
			type: 'string',
			default: 'WINT',
		},
		{
			displayName: 'User',
			name: 'user',
			type: 'string',
			default: 'system',
		},
		{
			displayName: 'Password',
			name: 'password',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
		},
		{
			displayName: 'Connect Timeout',
			name: 'connectTimeout',
			type: 'number',
			default: 15000,
			description: 'Connection timeout in ms',
		},
		{
			displayName: 'Request Timeout',
			name: 'requestTimeout',
			type: 'number',
			default: 15000,
			description: 'Request timeout in ms',
		},
	];
}
