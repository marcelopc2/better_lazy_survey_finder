import streamlit as st
import requests
from decouple import config
import pandas as pd
import unicodedata
import re
import time
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed

st.set_page_config(
    layout="wide",
    page_title="DATA SURVEY FINDER RELOADED",
    page_icon="🗳️"
)

BASE_URL = config("URL")
API_TOKEN = config("TOKEN")
HEADERS = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Content-Type": "application/json"
}

def clean_string(input_string: str) -> str:
    cleaned = input_string.strip().lower()
    cleaned = unicodedata.normalize('NFD', cleaned)
    cleaned = re.sub(r'[^\w\s.,!?-]', '', cleaned)
    cleaned = re.sub(r'[\u0300-\u036f]', '', cleaned)
    return cleaned

def canvas_request(session, method, endpoint, payload=None, paginated=False):
    if not BASE_URL:
        raise ValueError("BASE_URL no está configurada.")
    url = f"{BASE_URL}{endpoint}"
    results = []
    try:
        while url:
            if payload is not None and method.upper() == "GET":
                response = session.request(method.upper(), url, params=payload, headers=HEADERS)
            else:
                response = session.request(method.upper(), url, json=payload, headers=HEADERS)
            if not response.ok:
                st.error(f"Error en la petición a {url} ({response.status_code}): {response.text}")
                return None
            data = response.json()
            if paginated:
                results.extend(data)
                url = response.links.get("next", {}).get("url")
            else:
                return data
        return results if paginated else None
    except requests.exceptions.RequestException as e:
        st.error(f"Excepción en la petición a {url}: {e}")
        return None

def parse_course_ids(text):
    ids = re.split(r'[\s,]+', text)
    ids = [i.strip() for i in ids if i.strip().isdigit()]
    return ids

def get_surveys(course_id, session):
    endpoint = f"/courses/{course_id}/quizzes"
    quizzes = canvas_request(session, "GET", endpoint, paginated=True)
    if not quizzes:
        return []
    surveys = [q for q in quizzes if q.get("quiz_type") in ("survey", "graded_survey")]
    return surveys

@st.cache_data(show_spinner=False)
def get_course_name(course_id):
    url = f"{BASE_URL}/courses/{course_id}"
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            return response.json().get("name", f"Curso {course_id}")
    except Exception:
        pass
    return f"Curso {course_id}"

session = requests.Session()

def generate_report(course_id, quiz_id, quiz_title):
    canvas_url = BASE_URL
    headers = HEADERS
    session_local = requests.Session()  # cada thread usa su propia sesión para evitar problemas
    report_url = f"{canvas_url}/courses/{course_id}/quizzes/{quiz_id}/reports"
    report_payload = {
        "quiz_report": {
            "report_type": "student_analysis",
            "includes_all_versions": True
        }
    }
    try:
        report_response = session_local.post(report_url, headers=headers, json=report_payload)
        if report_response.status_code not in (200, 201):
            return None, f"[{quiz_title}] Error al solicitar la generación del reporte ({report_response.status_code})."

        report = report_response.json()
        report_id = report['id']
        status_url = report['progress_url']

        # Esperar a que el reporte esté listo (máx 2 min)
        for _ in range(120):
            progress_response = session_local.get(status_url, headers=headers)
            progress = progress_response.json()
            if progress.get('workflow_state') == 'completed':
                break
            time.sleep(2)
        else:
            return None, f"[{quiz_title}] El reporte demoró demasiado en generarse."

        report_status_url = f"{canvas_url}/courses/{course_id}/quizzes/{quiz_id}/reports/{report_id}"
        report_status_response = session_local.get(report_status_url, headers=headers)
        if report_status_response.status_code != 200:
            return None, f"[{quiz_title}] Error al obtener el estado del reporte."

        report_data = report_status_response.json()
        file_url = report_data['file']['url']

        file_response = requests.get(file_url)
        if file_response.status_code != 200:
            return None, f"[{quiz_title}] Error al descargar el archivo del reporte."

        df = pd.read_csv(BytesIO(file_response.content))
        df['Curso_ID'] = course_id
        df['Encuesta'] = quiz_title
        return df, None
    except Exception as exc:
        return None, f"[{quiz_title}] Excepción al generar reporte: {exc}"

def generar_reportes_en_paralelo(encuestas, show_progress=True):
    resultados = []
    errores = []
    total = len(encuestas)
    progress_bar = st.progress(0) if show_progress else None
    with ThreadPoolExecutor(max_workers=4) as executor:
        future_to_encuesta = {
            executor.submit(generate_report, e['course_id'], e['id'], e['title']): e
            for e in encuestas
        }
        for idx, future in enumerate(as_completed(future_to_encuesta)):
            encuesta = future_to_encuesta[future]
            try:
                df, err = future.result()
                if df is not None:
                    resultados.append(df)
                if err:
                    errores.append(err)
            except Exception as exc:
                errores.append(f"Error procesando {encuesta['title']} ({encuesta['course_id']}): {exc}")
            if show_progress and progress_bar:
                progress_bar.progress((idx + 1) / total)
    if show_progress and progress_bar:
        progress_bar.empty()
    return resultados, errores

def get_students_count(course_id, session):
    """Cuenta estudiantes activos inscritos en el curso."""
    endpoint = f"/courses/{course_id}/enrollments?type[]=StudentEnrollment&state[]=active&per_page=100"
    students = canvas_request(session, "GET", endpoint, paginated=True)
    if not students:
        return 0
    return len([s for s in students if s.get('user', {}).get('name') != 'Test Student'])

def get_quiz_submissions_count(course_id, quiz_id, session):
    """Cuenta encuestas enviadas (contestadas) por estudiantes."""
    endpoint = f"/courses/{course_id}/quizzes/{quiz_id}/submissions?per_page=100"
    submissions = canvas_request(session, "GET", endpoint, paginated=False)
    if not submissions:
        return 0
    # Canvas generalmente devuelve un dict con 'quiz_submissions'
    if isinstance(submissions, dict):
        quiz_submissions = submissions.get("quiz_submissions", [])
    else:
        quiz_submissions = []
    # Cuenta solo una vez por user_id (alumno) si existe submitted_at o finished_at
    user_ids = set()
    for s in quiz_submissions:
        if isinstance(s, dict) and (s.get("submitted_at") or s.get("finished_at")) and s.get("user_id"):
            user_ids.add(s["user_id"])
    return len(user_ids)

def obtener_participacion_encuesta(curso, encuesta, quiz_id, session):
    alumnos = get_students_count(curso, session)
    contestadas = get_quiz_submissions_count(curso, quiz_id, session)
    no_contestadas = alumnos - contestadas if alumnos >= contestadas else 0
    pct_contestadas = f"{(contestadas/alumnos*100):.1f}%" if alumnos > 0 else "0%"
    pct_no_contestadas = f"{(no_contestadas/alumnos*100):.1f}%" if alumnos > 0 else "0%"
    return {
        "Curso_ID": curso,
        "Encuesta": encuesta,
        "Alumnos Inscritos": alumnos,
        "Contestadas": contestadas,
        "% Contestadas": pct_contestadas,
        "No contestadas": no_contestadas,
        "% No contestadas": pct_no_contestadas,
    }

def order_resultados(resultados, ids):
    def get_index(df):
        cid = str(df['Curso_ID'].iloc[0]) if 'Curso_ID' in df.columns else None
        try:
            return ids.index(cid)
        except Exception:
            return 9999  # Al final si no existe
    return sorted(resultados, key=get_index)

# -------- UI PRINCIPAL --------

st.title("DATA SURVEY FINDER RELOADED 🗳️")
st.write("Ingresa los IDs de los cursos separados por coma, espacio o salto de línea:")

input_ids = st.text_area("IDs de cursos", height=100, placeholder="12345, 67890\n11223")

if "surveys_data" not in st.session_state:
    st.session_state.surveys_data = None

if st.button("Buscar Encuestas"):
    with st.spinner("Buscando encuestas..."):
        ids = parse_course_ids(input_ids)
        if not ids:
            st.warning("No se detectaron IDs válidos.")
            st.session_state.surveys_data = None
        else:
            surveys_by_course = {}
            all_surveys = []
            for course_id in ids:
                surveys = get_surveys(course_id, session)
                surveys_by_course[course_id] = surveys
                if surveys:
                    for s in surveys:
                        all_surveys.append({
                            "course_id": course_id,
                            "title": s["title"],
                            "id": s["id"],
                            "quiz_type": s.get("quiz_type"),
                        })
            st.session_state.surveys_data = {
                "by_course": surveys_by_course,
                "all": all_surveys,
                "ids": ids  # Guarda el orden
            }
    # Limpiar estado del reporte al hacer nueva búsqueda
    st.session_state.report_ready = False
    st.session_state.report_excel = None
    st.session_state.report_errors = None

if st.session_state.surveys_data and st.session_state.surveys_data["all"]:
    all_surveys = st.session_state.surveys_data["all"]
    ids = st.session_state.surveys_data["ids"]
    course_names = {cid: get_course_name(cid) for cid in ids}

    unique_titles = sorted(set(s["title"] for s in all_surveys))

    st.markdown("### Selecciona los nombres de encuesta que necesitas analizar")
    seleccionadas = []
    selected_titles = set()
    for idx, ut in enumerate(unique_titles):   # <-- ESTA LÍNEA USA idx PARA QUE EL KEY SEA ÚNICO
        group_key = f"select_{clean_string(ut)}_{idx}"  # <-- KEY SIEMPRE ÚNICO
        selected = st.checkbox(f"Seleccionar: '{ut}'", key=group_key)
        if selected:
            selected_titles.add(ut)
            seleccionadas.extend([s for s in all_surveys if s["title"] == ut])

    st.markdown("---")
    st.markdown("### Encuestas encontradas por curso")
    for course_id in ids:
        surveys = st.session_state.surveys_data["by_course"].get(course_id, [])
        st.subheader(course_names.get(course_id, f"Curso {course_id}"))
        if surveys:
            for s in surveys:
                selected_mark = "✅" if s["title"] in selected_titles else ""
                st.write(f"{selected_mark} {s['title']} (ID: {s['id']}")#, Tipo: {s.get('quiz_type')})")
        else:
            st.info("No se encontraron encuestas en este curso.")

    st.markdown("---")
    total = len(seleccionadas)
    st.info(f"Encuestas Seleccionadas\nTotal seleccionadas: {total}")

    if total > 0:
        resumen_por_curso = {cid: [] for cid in ids}
        with st.spinner("Recopilando datos de participación en las encuestas..."):
            participaciones = []
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [
                    executor.submit(
                        obtener_participacion_encuesta,
                        s["course_id"],
                        s["title"],
                        s["id"],
                        session
                    )
                    for s in seleccionadas
                ]
                for future in as_completed(futures):
                    participaciones.append(future.result())
            # Organizar por curso en el orden de entrada
            for part in participaciones:
                resumen_por_curso[part["Curso_ID"]].append(part)

        # Mostrar los cursos en el orden ingresado, con nombre real
        for curso in ids:
            if resumen_por_curso[curso]:
                st.markdown(f"**{course_names.get(curso, f'Curso {curso}')}:**")
                df = pd.DataFrame(resumen_por_curso[curso]).drop(columns=["Curso_ID"])
                df.reset_index(drop=True, inplace=True)
                st.dataframe(df, use_container_width=True, hide_index=True)

        if st.button("Generar reporte general"):
            with st.spinner("Generando reportes de encuestas..."):
                resultados, errores = generar_reportes_en_paralelo(seleccionadas)

                # Creamos un mapping (clave única: curso+encuesta) -> df
                resultados_map = {}
                for df in resultados:
                    if not df.empty:
                        # Usamos ambas llaves para distinguir
                        k = (str(df['Curso_ID'].iloc[0]), str(df['Encuesta'].iloc[0]))
                        resultados_map[k] = df

                output = BytesIO()
                with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                    startrow = 0
                    # Vamos por todas las seleccionadas, en el mismo orden y cantidad seleccionada
                    for idx, s in enumerate(seleccionadas):
                        curso = str(s["course_id"])
                        encuesta = str(s["title"])
                        curso_name = course_names.get(curso, f"Curso {curso}")
                        sheet = 'Reportes'
                        if idx == 0:
                            writer.sheets[sheet] = writer.book.add_worksheet(sheet)
                        ws = writer.sheets[sheet]

                        ws.write(startrow, 0, f"Encuesta: {encuesta} | Curso: {curso_name}")
                        startrow += 1

                        key = (curso, encuesta)
                        df = resultados_map.get(key)
                        if df is not None and not df.empty:
                            df.to_excel(writer, index=False, startrow=startrow, sheet_name=sheet, header=True)
                            startrow += len(df) + 2
                        else:
                            ws.write(startrow, 0, "Sin datos para esta encuesta.")
                            startrow += 3  # Deja un espacio
                st.session_state.report_excel = output.getvalue()
                st.session_state.report_ready = True
                st.session_state.report_errors = errores

        if st.session_state.get('report_ready', False) and st.session_state.get('report_excel', None):
            st.success("¡El reporte esta listo para descargar!")
            st.download_button(
                label="📥 Descargar Reporte General",
                data=st.session_state.report_excel,
                file_name="reporte_general_encuestas.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            if st.session_state.get('report_errors'):
                for e in st.session_state['report_errors']:
                    st.warning(e)
        elif st.session_state.get('report_errors'):
            for e in st.session_state['report_errors']:
                st.warning(e)
    else:
        st.write("No se seleccionaron encuestas.")
