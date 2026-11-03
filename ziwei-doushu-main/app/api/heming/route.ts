import { NextRequest } from 'next/server';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

const DEEPSEEK_URL = 'https://api.deepseek.com/chat/completions';
const encoder = new TextEncoder();

function errorResponse(message: string, status: number) {
  return Response.json({ error: message }, { status, headers: { 'Cache-Control': 'no-store' } });
}

type RecordValue = Record<string, unknown>;
function compactChart(value: unknown, label: string) {
  if (!value || typeof value !== 'object') throw new Error(`${label}命盘不存在`);
  const chart = value as RecordValue;
  const birth = chart.birthInfo as RecordValue | undefined;
  if (!birth || !Array.isArray(chart.palaces) || chart.palaces.length !== 12) {
    throw new Error(`${label}命盘格式不正确`);
  }
  const palaces = chart.palaces.map((item: unknown) => {
    const p = item as RecordValue;
    return {
      name: p.name, branch: p.branch, isMingGong: p.isMingGong,
      isShenGong: p.isShenGong, daXianAge: p.daXianAge,
      selfSihua: p.selfSihua, borrowedStars: p.borrowedStars,
      stars: Array.isArray(p.stars) ? p.stars.map((item: unknown) => {
        const st = item as RecordValue;
        return { name: st.name, type: st.type, siHua: st.siHua, brightness: st.brightnessRaw };
      }) : [],
    };
  });
  return {
    birth: { year: birth.year, month: birth.month, day: birth.day, hour: birth.hour, gender: birth.gender },
    lunar: chart.lunarInfo,
    mingZhu: chart.mingZhu, shenZhu: chart.shenZhu,
    wuxingJu: chart.wuxingJuName, daXian: chart.daXians,
    palaces,
  };
}

export async function POST(req: NextRequest) {
  const key = process.env.DEEPSEEK_API_KEY;
  if (!key) return errorResponse('缺少 DEEPSEEK_API_KEY 环境变量', 503);

  let chartA: ReturnType<typeof compactChart>;
  let chartB: ReturnType<typeof compactChart>;
  let question: string;
  try {
    if (Number(req.headers.get('content-length') || 0) > 250_000) return errorResponse('请求过大', 413);
    const body: unknown = await req.json();
    if (!body || typeof body !== 'object') throw new Error('无效请求');
    const data = body as RecordValue;
    chartA = compactChart(data.chartA, '甲方');
    chartB = compactChart(data.chartB, '乙方');
    if (data.question !== undefined && typeof data.question !== 'string') throw new Error('问题格式无效');
    question = typeof data.question === 'string' && data.question.trim()
      ? data.question.slice(0, 700)
      : '请从双方个性、关系互动、沟通冲突、互补点、相处建议等角度分析两张命盘，并强调这只是传统文化解释，不给出绝对匹配百分比。';
  } catch (e) {
    return errorResponse(e instanceof Error ? e.message : '数据校验失败', 400);
  }

  const system = `你是一名中文紫微斗数文化知识讲解助手，请对两张已经计算好的命盘做比较式合盘解读。
要求：仅根据输入的甲乙宫位、主星、四化以及大限做解释，不凭空编造星曜；区分甲方与乙方，不把两人盘混淆；以传统文化视角分析性格特点、相处互补、冲突与沟通方式、关系中的实际建议。
不要给“缘分注定”“一定结婚”“必然分手”等确定性预测，不伪造古籍原话或倪海厦逐字引文，不将星盘当作科学事实，也不以命理替代健康、法律、婚姻或财务判断。
以小标题 **【双方特点】**、**【互动与互补】**、**【可能的冲突】**、**【相处建议】** 输出。对提问可以集中回答相关内容。避免重复披露出生身份信息。
甲方命盘：${JSON.stringify(chartA)}
乙方命盘：${JSON.stringify(chartB)}`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 55_000);
  let upstream: Response;
  try {
    upstream = await fetch(DEEPSEEK_URL, {
      method: 'POST',
      headers: { Authorization: `Bearer ${key}`, 'Content-Type': 'application/json' },
      body: JSON.stringify({ model: process.env.DEEPSEEK_MODEL || 'deepseek-chat',
        stream: true, temperature: 0.5, max_tokens: 2000,
        messages: [{ role: 'system', content: system }, { role: 'user', content: question }] }),
      signal: controller.signal, cache: 'no-store',
    });
  } catch {
    clearTimeout(timer);
    return errorResponse('DeepSeek 暂时无法连接', 502);
  }
  if (!upstream.ok || !upstream.body) {
    clearTimeout(timer);
    return errorResponse(`AI 服务返回 ${upstream.status}，请检查额度及模型设置`, 502);
  }

  const stream = new ReadableStream<Uint8Array>({
    async start(output) {
      const reader = upstream.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let ended = false;
      try {
        while (!ended) {
          const { value, done } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';
          for (const rawLine of lines) {
            const line = rawLine.trim();
            if (!line.startsWith('data:')) continue;
            const data = line.slice(5).trim();
            if (data === '[DONE]') { ended = true; break; }
            try {
              const delta = JSON.parse(data).choices?.[0]?.delta?.content;
              if (typeof delta === 'string' && delta) {
                output.enqueue(encoder.encode(`data: ${JSON.stringify({ delta: { text: delta } })}\n\n`));
              }
            } catch { /* ignore malformed upstream frame */ }
          }
        }
      } catch {
        output.enqueue(encoder.encode(`data: ${JSON.stringify({ delta: { text: '\n[连接中断，请重试]' } })}\n\n`));
      } finally {
        clearTimeout(timer);
        reader.releaseLock();
        output.enqueue(encoder.encode('data: [DONE]\n\n'));
        output.close();
      }
    },
    cancel() { controller.abort(); clearTimeout(timer); },
  });
  return new Response(stream, { headers: {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-cache, no-transform',
    'X-Content-Type-Options': 'nosniff',
  } });
}
