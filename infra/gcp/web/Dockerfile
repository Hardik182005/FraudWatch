# Static FraudWatch site (Live or Lab). The deploy script copies a prebuilt Vite dist/ next to this file.
FROM nginx:1.29-alpine
COPY nginx.conf /etc/nginx/templates/default.conf.template
COPY 40-fraudwatch-config.sh /docker-entrypoint.d/40-fraudwatch-config.sh
COPY dist/ /usr/share/nginx/html/
RUN chmod +x /docker-entrypoint.d/40-fraudwatch-config.sh \
 && chown -R nginx:nginx /usr/share/nginx/html /var/cache/nginx /etc/nginx/conf.d \
 && touch /var/run/nginx.pid && chown nginx:nginx /var/run/nginx.pid
ENV PORT=8080
USER nginx
