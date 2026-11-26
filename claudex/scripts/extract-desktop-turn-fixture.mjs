// Run with bun; extracts the installed desktop's final turn request expression.
// The state below supplies inputs to that expression, not a rewritten payload.
import fs from 'node:fs';
import crypto from 'node:crypto';

const path = process.argv[2];
if (!path) throw new Error('Pass the extracted app-shared JavaScript bundle');
const source = fs.readFileSync(path, 'utf8');
const current = source.includes('let Fe={threadId:t,disabledPluginIds:');
const builderStart = current ? 0 : source.indexOf('function zln(');
const marker = current ? 'let Fe=' : 'let ze=';
const start = source.indexOf(`${marker}{threadId:`, builderStart);
const end = source.indexOf(';e.logger.info(', start);
if (builderStart < 0 || start < 0 || end < 0) throw new Error('Desktop builder changed; inspect before regenerating');
const expression = source.slice(start + marker.length, end);
if (!expression.endsWith('}')) throw new Error('Unexpected request expression boundary');
const state = current ? {
  t:'$THREAD_ID', c:{input:[{type:'text',text:'desktop payload',text_elements:[]}],turnTrigger:'submit',disabledPluginIds:[]},
  o:{pendingDisabledPluginIds:[],searchMcpContext:null}, O:null,
  i:'desktop-client-message', a:{}, De:null, Oe:false, ce:'$CWD',
  ve:true, _e:false, re:'on-request', z:false, oe:'auto_review',
  ye:null, we:true, me:{type:'workspaceWrite',writableRoots:['$CWD'],networkAccess:true,excludeTmpdirEnvVar:false,excludeSlashTmp:false},
  Ee:['$CWD'], F:'opus', Me:null, I:'medium', lj:'explicitRequestOnly', Pe:'none', Ne:'pragmatic', E:'projectless',
  j:{mode:'default',settings:{model:'opus',reasoning_effort:'medium',developer_instructions:null}},
  ew:(metadata, context)=>{if(context != null) throw new Error('Unexpected search context'); return metadata;},
} : {
  s: {input:[{type:'text',text:'desktop payload',text_elements:[]}],turnTrigger:'submit',disabledPluginIds:[]},
  t:'$THREAD_ID', r:'desktop-client-message', i:{}, a:{}, D:null,
  je:null, Me:false, de:'$CWD', Se:true, xe:true, R:'on-request',
  re:false, le:'user', Ce:null, Oe:true, ve:{type:'dangerFullAccess'}, Ae:['$CWD'],
  F:'fable', Ie:null, ee:'max', Kln:'explicitRequestOnly', Re:'detailed', Le:'pragmatic',
  A:{mode:'default',settings:{model:'fable',reasoning_effort:'max',developer_instructions:null}}, T:'project',
  // No search MCP context is supplied in this fixture; this is trn's first branch.
  trn:(metadata, context)=>{if(context != null) throw new Error('Unexpected search context'); return metadata;},
};
const evaluate = new Function(...Object.keys(state), `return (${expression});`);
const request = JSON.parse(JSON.stringify(evaluate(...Object.values(state)), (_, v)=>v === undefined ? null : v));
const fixture = {
  source: path.split('/').at(-1),
  sourceSha256:crypto.createHash('sha256').update(source).digest('hex'),
  expressionSha256:crypto.createHash('sha256').update(expression).digest('hex'),
  request,
};
fs.writeFileSync('tests/fixtures/desktop-turn.json', JSON.stringify(fixture,null,2)+'\n');
console.log(`Extracted ${Object.keys(request).length} turn fields from the installed desktop request expression`);
